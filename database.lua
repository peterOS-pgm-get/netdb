local Logger = pos.require('logger')
local sha256 = pos.require("hash.sha256")

local defCfg = {
    isServer = false,
    server = {
        root = '/home/netdb/',
        index = 'index.json',
        port = 10031,
        serverdb = 'netdb',
        userCtrl = false
    },
    port = 10031
}

local internalDBSchema = {
    ['users'] = {
        password = {
            type = "string",
            unique = false
        },
        access = {
            type = "string",
            unique = false
        },
        origin = {
            type = "string",
            unique = false
        },
        perms = {
            type = "string",
            unique = false
        },
        name = {
            type = "string",
            unique = false
        }
    }
}

---NetDB module
_G.netdb = {
    isSetup = false,
    config = defCfg,
    server = {}
}

---@class DBUser
---@field name string User name
---@field password string Password hash (SHA256)
---@field access {string: boolean} Databases the user can access
---@field origin {string: boolean} Where the user can connect from
---@field perms {string: boolean} Method permissions
local DBUser = {}
local DefaultDBUser = {
    name = "",
    password = "",
    access = {["*"]=true},
    origin = {["*"]=true},
    perms = {["*"]=true}
}
setmetatable(DefaultDBUser, {__index = DBUser})

---Converts database return to DBUser
---@param user table
---@return DBUser user
function DBUser.parse(user)
    local userOut = {}
    setmetatable(user, { __index = DBUser })
    if user.access ~= '*' and type(user.access) == 'string' then
        local t = string.split(user.access, ',')
        userOut.access = {}
        for _, db in pairs(t) do
            userOut.access[db] = true
        end
    elseif user.access == '*' then
        userOut.access = {
            ['*'] = true
        }
    end
    if user.perms ~= '*' and type(user.perms) == 'string' then
        local t = string.split(user.perms, ',')
        userOut.perms = {}
        for _, perm in pairs(t) do
            userOut.perms[perm] = true
        end
    elseif user.perms == '*' then
        userOut.perms = {
            ['*'] = true
        }
    end
    if user.origin ~= '*' and type(user.origin) == 'string' then
        local t = string.split(user.origin, ',')
        userOut.origin = {}
        for _, origin in pairs(t) do
            userOut.origin[origin] = true
        end
    elseif user.origin == '*' then
        userOut.origin = {
            ['*'] = true
        }
    end
    return userOut
end

---Returns if the user can access the specified database
---@param database string
---@return boolean can
function DBUser:canAccess(database)
    return self.access['*'] or self.access[database]
end

---Checks if the specified origin is valid for the user (String IP or HW address)
---@param origin string
---@return boolean valid
function DBUser:validOrigin(origin)
    return self.origin['*'] or self.origin[origin]
end

---Checks if the user has the specified method permission
---@param perm string
---@return boolean has
function DBUser:hasPerm(perm)
    return self.perms['*'] or self.perms[perm]
end

local cfgPath = '/home/.appdata/netdb/netdb.cfg'

local log = Logger('/home/.pgmLog/netdb.log')

local function fillDef(cfg, def)
    local bad = false
    for k, v in pairs(def) do
        if not cfg[k] then
            cfg[k] = v
            bad = true
        end
        if type(v) == 'table' then
            bad = bad or fillDef(cfg[k], v)
        end
    end
    return bad
end

dofile('DBConnector.lua')

---Setup NetDB module
---@return boolean success
function netdb.setup()
    if netdb.isSetup then
        return true
    end
    log:info('Starting NetDB')
    if not net.setup() then
        log:fatal('Net Module could not be started')
        return false
    end

    if fs.exists(cfgPath) then
        log:info('Attempting to load config')
        local f = fs.open(cfgPath, 'r')
        if not f then
            log:fatal('Could not access config file')
            error('Failed to start: Could not read config file', 0)
            return false
        end
        local cfg = textutils.unserialiseJSON(f.readAll())
        f.close()
        if not cfg then
            log:fatal('Config file malformed')
            error('Failed to start: Config malformed', 0)
            return false
        end
        if fillDef(cfg, defCfg) then
            log:error('Config was missing values, updating it')
            local cfgF = fs.open(cfgPath, 'w')
            if not cfgF then
                log:error('Could not make config file')
                error('Could not make config file', 0)
                return false
            end
            cfgF.write(textutils.serialiseJSON(cfg))
            cfgF.close()
        end
        netdb.config = cfg
        log:info('Loaded configuration')
    else
        log:warn('Config could not be found, creating one')
        local f = fs.open(cfgPath, 'w')
        if not f then
            log:error('Could not make config file')
            error('Could not make config file', 0)
            return false
        end
        f.write(textutils.serialiseJSON(netdb.config))
        f.close()
    end
    netdb.isSetup = true

    if netdb.config.isServer then
        netdb.server.start()
    end

    log:info('NetDB setup')
    return true
end

---Open a connection to a networked NetDB server
---@param server string|integer Server hostname or IP address, set to 'localhost' for local db
---@param db string Database name
---@param port integer|nil OPTIONAL connection port (default 10031)
---@return DBConnector|nil con Connection object OR <code>nil</code> on connection failure
function netdb.open(server, db, port)
    netdb.setup()
    log:info('Opening connection to Database')
    if not netdb.isSetup then
        if not netdb.setup() then
            log:error('Could not connect to Database, NetDB could not be setup')
            return nil
        end
    end
    port = port or netdb.config.port
    
    local o = netdb.createConnector(server, db, port, log)
    log:debug('Opening connection to: ' .. o.server .. ':' .. o.port .. ';Database=' .. o.db)
    if not o:ping() then
        log:error('Could not connect to Database')
        return nil
    end

    log:info('Connected to database')
    return o
end

local server = {
    indexFile = netdb.config.server.root .. netdb.config.server.index
}

local function updateDb(database, db)
    if not db then
        return
    end
    if not db._schema then
        db._schema = {}
        for name, table in pairs(db) do
            if name ~= '_schema' then
                local schema = {}
                db._schema[name] = schema
                for col, val in pairs(table[1]) do
                    schema[col] = {
                        type = type(val),
                        def = nil,
                        unique = false
                    }
                end
            end
        end
    end
    netdb.server.saveDb(database, db)
    return db
end

---Database server message handler
---@param msg NetMessage
local function serverHandler(msg)
    -- print('MSG: '..net.stringMessage(msg))
    if msg.port ~= netdb.config.server.port then return end
    if msg.header.type ~= 'netdb' then return end
    -- print('MSG was for server')

    local method = msg.header.method
    if method == 'ping' then
        msg:reply(netdb.config.server.port,
            { type = 'netdb', method = 'return', suc = true },
            {}
        )
        log:debug('Got pinged by ' .. net.ipFormat(msg.origin))
        return
    end

    local user = DefaultDBUser ---@type DBUser
    if netdb.config.server.userCtrl then
        if not msg.body.user then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Must specify user' }
            )
            return
        end
        local pHash = sha256.hash(msg.body.user.password)
        local s, r = netdb.server.run(netdb.config.server.serverdb,
            'SELECT * FROM users WHERE name="' .. msg.body.user.name .. '", password="' .. pHash .. '"')
        if not s or #r == 0 then
            if not s then
                log:error('User Val error: ' .. r)
            end
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'User Validation Error' }
            )
            return
        end
        user = DBUser.parse(r[1])
    end

    local dbName = msg.header.db
    if not user:canAccess(dbName) then
        msg:reply(netdb.config.server.port,
            { type = 'netdb', method = 'return', suc = false },
            { error = 'User can not access database' }
        )
        return
    end
    if not user:validOrigin(net.ipFormat(msg.origin)) then
        msg:reply(netdb.config.server.port,
            { type = 'netdb', method = 'return', suc = false },
            { error = 'Invalid origin for user' }
        )
        return
    end

    if not server.index.dbs[dbName] then
        msg:reply(netdb.config.server.port,
            { type = 'netdb', method = 'return', suc = false },
            { error = 'Database does not exist' }
        )
        return
    end

    log:debug('Msg')
    if method == 'get' then
        if not user:hasPerm('select') then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Invalid permissions: SELECT' }
            )
        end
        local s, r = netdb.server.get(dbName, msg.body.table, msg.body.sel.cols, msg.body.sel.vals, msg.body.cols)
        if s then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = true },
                r
            )
        else
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = r }
            )
        end
        return
    elseif method == 'put' then
        if not user:hasPerm('update') then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Invalid permissions: UPDATE' }
            )
        end
        local s, r = netdb.server.put(dbName, msg.body.table, msg.body.sel.cols, msg.body.sel.vals, msg.data.cols,
            msg.data.vals)
        if s then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = true },
                { r }
            )
        else
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = r }
            )
        end
        return
    elseif method == 'insert' then
        if not user:hasPerm('insert') then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Invalid permissions: INSERT' }
            )
        end
        local s, r = netdb.server.insert(dbName, msg.body.table, msg.body.cols, msg.body.vals)
        if s then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = true },
                { r }
            )
        else
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = r }
            )
        end
        return
    elseif method == 'exists' then
        if not user:hasPerm('exists') then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Invalid permissions: EXISTS' }
            )
        end
        local s, r = netdb.server.exists(dbName, msg.body.table, msg.body.cols, msg.body.vals)
        if s then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = true },
                { r }
            )
        else
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = r }
            )
        end
        return
    elseif method == 'run' then
        local m = string.lower(string.split(msg.body.cmd, ' ')[1])
        if not user:hasPerm(m) then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = 'Invalid permissions: ' .. string.upper(m) }
            )
        end
        log:debug('Cmd: ' .. string.sub(msg.body.cmd, 1, math.min(string.len(msg.body.cmd), 64)))
        local s, r = netdb.server.run(dbName, msg.body.cmd)
        if s then
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = true },
                r
            )
            log:debug(' - Suc')
        else
            msg:reply(netdb.config.server.port,
                { type = 'netdb', method = 'return', suc = false },
                { error = r }
            )
            log:debug(' - Fail: ' .. r)
        end
        return
    end

    msg:reply(netdb.config.server.port,
        { type = 'netdb', method = 'return', suc = false },
        { error = 'Unknown method' }
    )
end

local defaultIndex = {
    dbs = {}
}

---Starts the NetDB server
---@return boolean started if the startup was successful
function netdb.server.start()
    if server.handler then
        return true
    end
    log:info('Starting NetDB server')
    if not netdb.isSetup then
        if not netdb.setup() then
            log:error('Could not start server, NetDB could not be setup')
            return false
        end
    end
    if fs.exists(server.indexFile) then
        local f = fs.open(server.indexFile, 'r')
        if not f then
            log:fatal('Could not access server index file')
            return false
        end
        server.index = textutils.unserialiseJSON(f.readAll())
        f.close()
        if fillDef(server.index, defaultIndex) then
            local indexF = fs.open(server.indexFile, 'w')
            if not indexF then
                log:fatal('Could not fix server index file')
                return false
            end
            indexF.write(textutils.serialiseJSON(defaultIndex))
            indexF.close()
            log:warn('Added missing entries to server index file')
        end
        log:info('Server index loaded')
    else
        local f = fs.open(server.indexFile, 'w')
        if not f then
            log:fatal('Could not create server index file')
            return false
        end
        f.write(textutils.serialiseJSON(defaultIndex))
        f.close()
        log:info('Default index created')
        server.index = defaultIndex
    end

    if not netdb.server.hasDb(netdb.config.server.serverdb) then
        netdb.server.createDatabase(netdb.config.server.serverdb)
        netdb.server.createTable(netdb.config.server.serverdb, 'users')
        local db = netdb.server.loadDb(netdb.config.server.serverdb)
        if not db then
            log:error('Could not write schema table to internal db')
            return false
        end
        db._schema = internalDBSchema
        netdb.server.saveDb(netdb.config.server.serverdb, db)
        netdb.server.run(netdb.config.server.serverdb,
            'INSERT INTO users name, password, access, perms, origin VALUES "root", "' ..
            sha256.hash('root') .. '", "*", "*", "*"')
    end

    server.handler = net.registerMsgHandler(serverHandler)
    net.open(netdb.config.server.port)
    log:info('NetDB server started')
    return true
end

---Updates the index file
function netdb.server.updateIndex()
    netdb.setup()
    local f = fs.open(server.indexFile, 'w')
    if not f then
        log:fatal('Could not update server index file')
        return
    end
    f.write(textutils.serialiseJSON(server.index))
    f.close()
    log:info('Server index file updated')
end

---Load a database by name
---@param database string database name
---@return table|nil db The database OR nil on failure
function netdb.server.loadDb(database)
    netdb.setup()
    if not server.index.dbs[database] then
        return nil
    end
    local df = fs.open(netdb.config.server.root .. database .. '.db', 'r')
    if not df then
        log:error('Could not load DB `' .. database .. '`')
        return nil
    end
    local db = textutils.unserialiseJSON(df.readAll())
    df.close()
    if not db then
        log:error('DB `' .. database .. '` corrupted')
        return nil
    end
    db = updateDb(database, db)
    return db
end

---Saves a database
---@param database string Database namer
---@param db table Database
---@return boolean success If the database was saved
function netdb.server.saveDb(database, db)
    netdb.setup()
    local df = fs.open(netdb.config.server.root .. database .. '.db', 'w')
    if not df then
        log:error('Could not save DB `' .. database .. '`')
        return false
    end
    df.write(textutils.serialiseJSON(db))
    df.close()
    return true
end

---Checks if the given columns and values are valid under the schema
---@param db table Database
---@param table string Table name
---@param cols table|nil List of columns
---@param vals table|nil OPTIONAL List of values for column
---@return boolean valid
---@return string error
local function validCols(db, table, cols, vals)
    if cols then
        for i, col in pairs(cols) do
            if not db._schema[table][col] then
                return false, 'Invalid column: `' .. col .. '`'
            end
            if vals and type(vals[i]) ~= db._schema[table][col].type then
                if vals[i] == nil and (db._schema[table][col].notNil) then
                    return false,
                        'Invalid type for column `' ..
                        col .. '`, must be ' .. db._schema[table][col].type .. ' but was ' .. type(vals[i])
                end
            end
        end
    end
    return true, ''
end

local function rowMatch(row, cols, vals)
    if not cols then
        return true
    end
    for i, col in pairs(cols) do
        if type(vals[i]) == 'table' then
            local inArr = false
            for _, v in pairs(vals[i]) do
                if row[col] == v then
                    inArr = true
                end
            end
            if not inArr then
                return false
            end
        else
            if row[col] ~= vals[i] then
                return false
            end
        end
    end
    return true
end

---Get data from the database
---@param database string Database name
---@param tableName string Table name
---@param sCols string[]|nil List of selector columns
---@param sVals any[]|nil List of selector values
---@param cols string[]|nil List of columns to get
---@return boolean success
---@return table|string rsp List of rows matching selector OR error description
function netdb.server.get(database, tableName, sCols, sVals, cols)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end
    local tbl = db[tableName]
    if not tbl then
        return false, 'Table does not exist'
    end

    local valid, err = validCols(db, tableName, sCols, sVals)
    if not valid then
        return false, err
    end
    valid, err = validCols(db, tableName, cols)
    if not valid then
        return false, err
    end

    local out = {}
    for _, row in pairs(tbl) do
        if rowMatch(row, sCols, sVals) then
            if not cols or #cols == 0 then
                table.insert(out, row)
            else
                local v = {}
                for _, c in pairs(cols) do
                    v[c] = row[c]
                end
                table.insert(out, v)
            end
        end
    end
    return true, out
end

---Put data into the database
---@param database string Database name
---@param tableName string Table name
---@param sCols string[] List of selector columns
---@param sVals any[] List of selector values
---@param dCols string[] List of data columns to set
---@param dVals any[] List of values to set columns to
---@return boolean success
---@return string rsp `'<#> rows updated'` OR error description
function netdb.server.put(database, tableName, sCols, sVals, dCols, dVals)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end
    local tbl = db[tableName]
    if not tbl then
        return false, 'Table does not exist'
    end

    local valid, err = validCols(db, tableName, sCols, sVals)
    if not valid then
        return false, err
    end
    valid, err = validCols(db, tableName, dCols, dVals)
    if not valid then
        return false, err
    end
    local unique = false
    for i, col in pairs(dCols) do
        if db._schema[tableName][col].unique then
            unique = true
            for _, row in pairs(tbl) do
                if row[col] == dVals[i] then
                    return false, 'Duplicate key for column `' .. col .. '`'
                end
            end
        end
    end
    local rCount = 0
    for _, row in pairs(tbl) do
        if rowMatch(row, sCols, sVals) then
            if rCount > 0 then
                return true, 'ERROR: Duplicate key for successive rows'
            end
            for i, col in pairs(dCols) do
                row[col] = dVals[i]
            end
            rCount = rCount + 1
        end
    end
    if not netdb.server.saveDb(database, db) then
        return false, 'Could not save Database'
    end
    return true, rCount .. ' rows updated'
end

---Insert a new row into the table
---@param database string Database name
---@param tableName string Table name
---@param cols string[] List of columns to set for the new row
---@param vals any[] List of data from columns
---@return boolean success
---@return string rsp `'Row inserted'` OR error description
function netdb.server.insert(database, tableName, cols, vals)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end
    local tbl = db[tableName]
    if not tbl then
        return false, 'Table does not exist'
    end

    local valid, err = validCols(db, tableName, cols, vals)
    if not valid then
        return false, err
    end

    for i, col in pairs(cols) do
        if db._schema[tableName][col].unique then
            for _, row in pairs(tbl) do
                if row[col] == vals[i] then
                    return false, 'Duplicate key for column `' .. col .. '`'
                end
            end
        end
    end
    for col, sch in pairs(db._schema[tableName]) do
        if sch.notNil or sch.def then
            local exists = false
            for _, c in pairs(cols) do
                if c == col then
                    exists = true
                end
            end
            if not exists then
                if sch.def then
                    table.insert(cols, col)
                    table.insert(vals, sch.def)
                    exists = true
                end
                if not exists and sch.notNil then
                    return false, 'Column `' .. col .. '` must not be nil'
                end
            end
        end
    end

    local v = {}
    for i, col in pairs(cols) do
        v[col] = vals[i]
    end
    table.insert(tbl, v)
    if not netdb.server.saveDb(database, db) then
        return false, 'Could not save Database'
    end
    return true, 'Row inserted'
end

---Check if a row matching selector exists
---@param database string Database name
---@param tableName string Table name
---@param cols string[] List of selector columns
---@param vals any[] List of selector values
---@return boolean success
---@return boolean|string rsp If a row existed OR error description
function netdb.server.exists(database, tableName, cols, vals)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end
    local tbl = db[tableName]
    if not tbl then
        return false, 'Table does not exist'
    end

    local valid, err = validCols(db, tableName, cols, vals)
    if not valid then
        return false, err
    end

    local exists = false
    for _, r in pairs(tbl) do
        local g = true
        for i, c in pairs(cols) do
            if r[c] ~= vals[i] then
                g = false
                break
            end
        end
        if g then
            exists = true
        end
    end
    return true, exists
end

---Delete data from the database
---@param database string Database name
---@param tableName string Table name
---@param sCols table List of selector columns
---@param sVals table List of selector values
---@return boolean success
---@return string rsp `'<#> rows deleted'` OR error description
function netdb.server.delete(database, tableName, sCols, sVals)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end
    local tbl = db[tableName]
    if not tbl then
        return false, 'Table does not exist'
    end

    local valid, err = validCols(db, tableName, sCols, sVals)
    if not valid then
        return false, err
    end
    local rCount = 0
    for i, row in pairs(tbl) do
        if rowMatch(row, sCols, sVals) then
            -- for i, col in pairs(dCols) do
            --     row[col] = dVals[i]
            -- end
            tbl[i] = nil
            rCount = rCount + 1
        end
    end
    if not netdb.server.saveDb(database, db) then
        return false, 'Could not save Database'
    end
    return true, rCount .. ' rows deleted'
end

---List all databases
---@return boolean success
---@return string[] dbs List of databases
function netdb.server.listDbs()
    netdb.setup()
    local list = {}
    for name, _ in pairs(server.index.dbs) do
        table.insert(list, name)
    end
    return true, list
end

---List tables in database
---@param database string Database name
---@return boolean success
---@return string[]|string rsp List of tables OR error string
function netdb.server.listTables(database)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end

    local list = {}
    for name, _ in pairs(db) do
        if not string.start(name, '_') then
            table.insert(list, name)
        end
    end
    return true, list
end

---Create a database
---@param name string Database name
---@return boolean success
---@return string rsp DB created string OR error string
function netdb.server.createDatabase(name)
    netdb.setup()
    if server.index.dbs[name] then
        return false, 'Database already exists'
    end
    local df = fs.open(netdb.config.server.root .. name .. '.db', 'w')
    if not df then
        return false, 'Could not create database'
    end
    local db = {
        _schema = {}
    }
    df.write(textutils.serialiseJSON(db))
    df.close()
    server.index.dbs[name] = true
    netdb.server.updateIndex()
    log:info('Created database `' .. name .. '`')
    return true, 'Database `' .. name .. '` created'
end

---Create a table
---@param database string Database name
---@param name string Table name
---@return boolean success
---@return string rsp Table created string OR error string
function netdb.server.createTable(database, name)
    netdb.setup()
    local db = netdb.server.loadDb(database)
    if not db then
        return false, 'Could not load Database'
    end

    db[name] = {}
    db._schema[name] = {}

    if not netdb.server.saveDb(database, db) then
        return false, 'Could not save Database'
    end
    log:info('Created table `' .. name .. '` in database `' .. database .. '`')
    return true, 'Table `' .. name .. '` created'
end

---Returns if a database exists with name
---@param database string Database name
---@return boolean exists If the database exists
function netdb.server.hasDb(database)
    return server.index.dbs[database] ~= nil
end

function netdb.server.getArgs(cmd)
    local parts = string.split(cmd, ' ')
    local inQuotes = false
    local temp = ''
    local arguments = {}
    local commands = {}
    local c = false
    local lc = false

    local keyVal = nil

    local par = nil
    local pSet = {}

    local function insert(val)
        if keyVal and inQuotes then
            keyVal.val = val
            val = keyVal
            keyVal = nil
        end
        if par then
            table.insert(pSet, val)
            return
        end
        if c or lc then
            table.insert(arguments[#arguments], val)
        else
            table.insert(arguments, val)
        end
        lc = c
    end

    for i = 1, #parts do
        local p = parts[i]
        local endCmd = false
        c = (not (inQuotes or par)) and string.sub(p, -1) == ','
        if c then
            p = string.sub(p, 1, -2)
            if not lc then table.insert(arguments, {}) end
        end
        if inQuotes then
            if (string.sub(p, -2) == '";') then
                endCmd = true
                p = string.sub(p, -1)
            end
            if string.sub(p, -2) == '",' then
                c = true
                p = string.sub(p, 1, -2)
            end
            temp = temp .. ' ' .. p
            if string.sub(p, -1) == '"' then
                insert(string.sub(temp, 2, -2))
                inQuotes = false
            end
        else
            if string.start(p, '"') then
                if (string.sub(p, -2) == '";') then
                    endCmd = true
                    p = string.sub(p, -1)
                end
                if string.sub(p, -1) == '"' then
                    insert(string.sub(p, 2, -2))
                else
                    inQuotes = true
                    temp = p
                end
            elseif string.len(p) > 1 and string.cont(p, '=') then
                if (string.sub(p, -1) == ';') then
                    endCmd = true
                    p = string.sub(p, -1)
                end
                local pts2 = string.split(p, '=')
                keyVal = {
                    key = pts2[1],
                    val = table.concat(pts2, '=', 2)
                }
                if string.sub(keyVal.val, -1) == '"' then
                    keyVal.val = string.sub(keyVal.val, 2, -2)
                    insert(keyVal)
                elseif string.start(keyVal.val, '"') then
                    inQuotes = true
                    temp = keyVal.val
                else
                    ---@diagnostic disable-next-line: assign-type-mismatch
                    keyVal.val = tonumber(keyVal.val) or keyVal.val
                    insert(keyVal)
                end
            elseif p == '(' then
                par = {}
            elseif par then
                if (string.sub(p, -1) == ';') then
                    endCmd = true
                    p = string.sub(p, -1)
                end
                if p == ')' then
                    table.insert(par, pSet)
                    local t = par
                    par = nil
                    insert(t)
                else
                    local parc = string.sub(p, -1) == ','
                    if parc then
                        p = string.sub(p, 1, -2)
                    end
                    insert(p)
                    if parc then
                        table.insert(par, pSet)
                        pSet = {}
                    end
                end
            else
                if (string.sub(p, -1) == ';') then
                    endCmd = true
                    p = string.sub(p, -1)
                end
                local n = tonumber(p)
                if n then
                    insert(n)
                else
                    insert(p)
                end
            end
        end
        if endCmd then
            table.insert(commands, arguments)
            arguments = {}
        end
    end
    if arguments and #arguments > 0 then
        table.insert(commands, arguments)
    end
    return commands
end

---Splits KV argument into separate lists
---@param arr any
---@return table|nil
---@return table|nil
function netdb.server.splitKv(arr)
    if not arr then
        return nil
    end
    local k, v = {}, {}
    if arr.key then
        k = { arr.key }
        v = { arr.val }
        return k, v
    end
    for i, kv in pairs(arr) do
        if kv[1] then
            if kv[2] == '=' then
                k[i] = kv[1]
                v[i] = kv[3]
            elseif string.lower(kv[2]) == 'in' then
                k[i] = kv[1]
                v[i] = {}
                local a = string.split(string.sub(kv[3], 2, -2), ',')
                for j, val in pairs(a) do
                    if string.start(val, '"') then
                        v[i][j] = string.sub(val, 2, -2)
                    elseif tonumber(val) then
                        v[i][j] = tonumber(val)
                    elseif val == 'true' then
                        v[i][j] = true
                    elseif val == 'false' then
                        v[i][j] = false
                    elseif val == 'nil' then
                        v[i][j] = nil
                    end
                end
            end
        else
            k[i] = kv.key
            v[i] = kv.val
        end
    end
    return k, v
end

local function fixArr(arr)
    if arr == nil then
        return {}
    end
    if type(arr) ~= 'table' then
        return { arr }
    end
    return arr
end

---Run SQL style commands(s)
---@param database string Database name
---@param cmd string SQL style command (can contain `;` for separating commands)
---@return boolean success
---@return any[]|any|string result List of results from every command (not enclosed if single command) OR first error message
function netdb.server.run(database, cmd)
    local commands = netdb.server.getArgs(cmd)
    if (#commands == 1) then
        return netdb.server.execute(database, commands[1])
    end
    local results = {}
    for i = 0, #commands do
        if #commands[i] > 0 then
            local s, r = netdb.server.execute(database, commands[i])
            if not s then
                return false, r
            end
            results[i] = r
        else
            results[i] = nil
        end
    end
    return true, results
end
---Run and SQL style command
---@param database string database name
---@param args table SQL style arguments
---@return boolean success
---@return any|string return command return or error string if failure
function netdb.server.execute(database, args)
    netdb.setup()
    args[1] = string.lower(args[1])
    if args[1] == 'show' then -- SHOW [DATABASE|TABLES|SCHEMA]
        args[2] = string.lower(args[2])
        if args[2] == 'database' then
            return netdb.server.listDbs()
        end
        if not netdb.server.hasDb(database) then
            return false, 'Database does not exist'
        end
        if args[2] == 'tables' then
            return netdb.server.listTables(database)
        elseif args[2] == 'schema' then
            local db = netdb.server.loadDb(database)
            if not db then
                return false, 'Database does not exist'
            end
            local table = args[3]
            if not db[table] then
                return false, 'Table does not exist'
            end
            return true, db._schema[table]
        end
    elseif args[1] == 'insert' then -- INSERT INTO table cols VALUES vals
        return netdb.server.insert(database, args[3], fixArr(args[4]), fixArr(args[6]))
    elseif args[1] == 'select' then -- SELECT cols FROM table WHERE condition
        local sel = args[2]
        if sel == '*' then
            sel = nil
        end
        local sCols, sVals
        if #args == 4 then
            -- no where
        elseif #args == 6 then
            sCols, sVals = netdb.server.splitKv(args[6])
        elseif #args == 8 then
            if args[7] == '=' then
                sCols = { args[6] }
                sVals = { args[8] }
            elseif string.lower(args[7]) == 'in' then
                sCols = { args[6] }
                sVals = { {} }
                -- local ai =
                local a = string.split(string.sub(args[8], 2, -2), ',')
                for i, v in pairs(a) do
                    if string.start(v, '"') then
                        sVals[1][i] = string.sub(v, 2, -2)
                    elseif tonumber(v) then
                        sVals[1][i] = tonumber(v)
                    elseif v == 'true' then
                        sVals[1][i] = true
                    elseif v == 'false' then
                        sVals[1][i] = false
                    elseif v == 'nil' then
                        sVals[1][i] = nil
                    end
                end
            else
                return false, 'Malformed select'
            end
        else
            return false, 'Malformed select'
        end
        return netdb.server.get(database, args[4], sCols, sVals, fixArr(sel))
    elseif args[1] == 'update' then -- UPDATE table SET cols=vals WHERE condition
        local sCols, sVals = netdb.server.splitKv(args[6])
        if not sCols or not sVals then
            return false, 'Missing selector'
        end
        local dCols, dVals = netdb.server.splitKv(args[4])
        if not dCols or not dVals then
            return false, 'Missing data'
        end
        return netdb.server.put(database, args[2], sCols, sVals, dCols, dVals)
    elseif args[1] == 'create' and string.lower(args[2]) == 'table' then -- CREATE TABLE table ( col type UNIQUE NOT_NIL PRIMARY_KEY def=default )
        local table = args[3]
        if string.start(table, '_') then
            return false, 'Invalid table name, can not start with an _'
        end
        local db = netdb.server.loadDb(database)
        if not db then
            return false, 'Database does not exist'
        end
        if db[table] then
            return false, 'Table already exists'
        end
        db[table] = {}
        db._schema[table] = {}
        for _, col in pairs(args[4]) do
            local schema = {
                type = col[2],
                def = nil,
                unique = false,
            }
            db._schema[table][col[1]] = schema
            for i = 3, #col do
                if col[i] == 'UNIQUE' then
                    schema.unique = true
                elseif col[i] == 'NOT_NIL' then
                    schema.notNil = true
                elseif col[i] == 'PRIMARY_KEY' then
                    schema.notNil = true
                    schema.unique = true
                elseif type(col[i]) == "table" then
                    if col[i].key == 'def' then
                        schema.def = col[i].val
                    end
                end
            end
        end
        netdb.server.saveDb(database, db)
        return true, 'Table `' .. table .. '` created'
    elseif args[1] == 'alter' and string.lower(args[2]) == 'table' then -- `ALTER TABLE table ADD column type UNIQUE NOT_NIL PRIMARY_KEY def=default` or `ALTER TABLE table DROP column` or `ALTER TABLE table MODIFY column type UNIQUE NOT_NIL PRIMARY_KEY def=default`
        local db = netdb.server.loadDb(database)
        if not db then
            return false, 'Database does not exist'
        end
        local table = args[3]
        if not db[table] then
            return false, 'Table does not exists'
        end

        local opt = string.lower(args[4])
        if opt == 'add' then
            local col = args[5]
            if db._schema[table][col] then
                return false, 'Column already exists'
            end
            local schema = {
                type = args[6],
                def = nil,
                unique = false,
            }
            db._schema[table][col] = schema
            for i = 7, #args do
                if args[i] == 'UNIQUE' then
                    schema.unique = true
                elseif args[i] == 'NOT_NIL' then
                    schema.notNil = true
                elseif args[i] == 'PRIMARY_KEY' then
                    schema.notNil = true
                    schema.unique = true
                elseif type(args[i]) == "table" then
                    if args[i].key == 'def' then
                        schema.def = args[i].val
                    end
                end
            end
            for _, row in pairs(db[table]) do
                row[col] = schema.def
            end
            netdb.server.saveDb(database, db)
            return true, 'Column added'
        elseif opt == 'DROP' then
            local col = args[5]
            if not db._schema[table][col] then
                return false, 'Column does not exists'
            end
            -- db[table][col] = nil
            for _, row in pairs(db[table]) do
                row[col] = nil
            end
            db._schema[table][col] = nil
            netdb.server.saveDb(database, db)
            return true, 'Column dropped'
        elseif opt == 'modify' then
            local col = args[5]
            if not db._schema[table][col] then
                return false, 'Column does not exists'
            end
            local schema = db._schema[table][col]
            for i = 7, #args do
                if args[i] == 'UNIQUE' then
                    schema.unique = true
                elseif args[i] == 'NOT_NIL' then
                    schema.notNil = true
                elseif args[i] == 'PRIMARY_KEY' then
                    schema.notNil = true
                    schema.unique = true
                elseif type(args[i]) == "table" then
                    if args[i].key == 'def' then
                        schema.def = args[i].val
                    end
                end
            end
            netdb.server.saveDb(database, db)
            return true, 'Column altered'
        end
        return true, 'Unknown alter type'
    elseif args[1] == 'get' then -- GET [SCHEMA|TABLES]
        local db = netdb.server.loadDb(database)
        if not db then
            return false, 'Database does not exist'
        end
        args[2] = string.lower(args[2])
        if args[2] == 'schema' then -- GET SCHEMA table
            local table = args[3]
            if not db[table] then
                return false, 'Table does not exist'
            end
            return true, db._schema[table]
        elseif args[2] == 'tables' then -- GET TABLES
            local tables = {}
            for name, _ in pairs(db._schema) do
                table.insert(tables, name)
            end
            return true, tables
        end
    elseif args[1] == 'delete' then -- DELETE FROM table WHERE condition
        local db = netdb.server.loadDb(database)
        if not db then
            return false, 'Database does not exist'
        end
        local sCols, sVals = netdb.server.splitKv(args[5])
        if not sCols or not sVals then
            return false, 'Missing selector'
        end
        return netdb.server.delete(database, args[3], sCols, sVals)
    end
    return false, 'Unknown action'
end

function netdb.printTbl(tbl)
    local str = '{ '
    local c = false
    for k, v in pairs(tbl) do
        if c then
            str = str .. ', '
        end
        str = str .. k .. ' = '
        if type(v) == 'string' then
            str = str .. '"' .. v .. '"'
        elseif type(v) == 'table' then
            str = str .. netdb.printTbl(v)
        else
            str = str .. tostring(v)
        end
        c = true
    end
    return str .. ' }'
end

---Get all users in database
---@return table|string rsp List of users OR error message
function netdb.server.getUsers()
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name, access, perms, origin FROM users')
    if not s then
        return 'ERROR: ' .. r
    end
    return r
end

---Add a user to the database
---@param name string New username
---@param password string Password for new user
---@return boolean success
---@return string rsp `'User added'` OR error message
function netdb.server.addUser(name, password)
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name FROM users WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    if #r > 0 then
        return false, 'User already exists'
    end
    s, r = netdb.server.run(netdb.config.server.serverdb,
        'INSERT INTO users name, password, access, perms, origin VALUES "' ..
        name .. '", "' .. sha256.hash(password) .. '", "*", "*", "*"')
    if not s then
        return false, r
    end
    return true, 'User added'
end

---Set the password of a user
---@param name string User name
---@param password string New password
---@return boolean success
---@return string rsp `'Password changed'` OR error message
function netdb.server.setUserPassword(name, password)
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name FROM users WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    if #r <= 0 then
        return false, 'User does not exists'
    end
    s, r = netdb.server.run(netdb.config.server.serverdb,
        'UPDATE users SET password="' .. sha256(password) .. '" WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    return true, 'Password changed'
end

---Set the permissions of a user
---@param name string User name
---@param perms string New permissions
---@return boolean success
---@return string rsp `'Perms changed'` OR error message
function netdb.server.setUserPerms(name, perms)
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name FROM users WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    if #r <= 0 then
        return false, 'User does not exists'
    end
    s, r = netdb.server.run(netdb.config.server.serverdb, 'UPDATE users SET perms="' ..
        perms .. '" WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    return true, 'Perms changed'
end

---Set the database access of a user
---@param name string User name
---@param access string New database access
---@return boolean success
---@return string rsp `'Access changed'` OR error message
function netdb.server.setUserAccess(name, access)
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name FROM users WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    if #r <= 0 then
        return false, 'User does not exists'
    end
    s, r = netdb.server.run(netdb.config.server.serverdb,
        'UPDATE users SET access="' .. access .. '" WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    return true, 'Access changed'
end

---Set the origin policy of a user
---@param name string User name
---@param origin string New origin policy
---@return boolean success
---@return string `'Origin changed'` OR error message
function netdb.server.setUserOrigin(name, origin)
    local s, r = netdb.server.run(netdb.config.server.serverdb, 'SELECT name FROM users WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    if #r <= 0 then
        return false, 'User does not exists'
    end
    s, r = netdb.server.run(netdb.config.server.serverdb,
        'UPDATE users SET origin="' .. origin .. '" WHERE name="' .. name .. '"')
    if not s then
        return false, r
    end
    return true, 'Origin changed'
end