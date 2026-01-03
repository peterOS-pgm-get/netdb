---@class NetDB.Server
---@field config NetDB.Server.Config
---@field __log Logger
---@field __dbs {[string]: string}
local Server = {}
local ServerMT = {
    __index = Server
}

---@type { [string]: NetDB.TableSchema }
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

---@class NetDB.TablesTableRow
---@field name string The name of the table (Primary Key)
---@field primaryKey string The primary key of the table
---@field recordIndex number The record index, used for non-keyed tables

---@class NetDB.TablesTable
---@field [string] NetDB.TablesTableRow

local TABLES_TABLE_NAME = '__tables'
---@type NetDB.TableSchema
local tablesTableSchema = {
    ['name'] = {
        type = "string",
        primaryKey = true,
        unique = true,
        notNil = true
    },
    ['primaryKey'] = {
        type = 'string',
        notNil = true
    },
    ['recordIndex'] = {
        type = 'number',
        notNil = true
    }
}

---@class NetDB.ColumnsTableRow
---@field _rId number The record ID (Unique)
---@field table string The name of the table the column is in
---@field column string The name of the column
---@field type string The column type
---@field unique boolean? If the column must have a unique value
---@field notNil boolean? If the column may not have a `nil` value
---@field primaryKey boolean? If the column is the primary key of the table
---@field def any? The column's default value, if any

local COLUMNS_TABLE_NAME = '__columns'

---@type NetDB.TableSchema
local columnsTableSchema = {
    ['_rId'] = {
        type = 'number',
        primaryKey = true,
        notNil = true,
        unique = true,
    },
    ['table'] = {
        type = 'string',
        notNil = true
    },
    ['column'] = {
        type = 'string',
        notNil = true
    },
    ['type'] = {
        type = 'string',
        notNil = true
    },
    ['unique'] = {
        type = 'boolean',
    },
    ['notNil'] = {
        type = 'boolean',
    },
    ['primaryKey'] = {
        type = 'boolean',
    },
    ['def'] = {
        type = 'any',
    }
}

local DEFAULT_RECORD_ID_COLUMN = '_rId'

---Add a table to the tables table
---@param tablesTable table
---@param tableName string
---@param primaryKey string
---@param recordIndex number
local function addTableToTablesTable(tablesTable, tableName, primaryKey, recordIndex)
    tablesTable[tableName] = { ---@type NetDB.TablesTableRow
        name = tableName,
        primaryKey = primaryKey,
        recordIndex = recordIndex
    }
end
---Create a new tables table
---@param colRI number The record index from the columns table
---@return table
---@nodiscard
local function createDefaultTablesTable(colRI)
    local o = {}
    addTableToTablesTable(o, TABLES_TABLE_NAME, 'name', 0)
    addTableToTablesTable(o, COLUMNS_TABLE_NAME, 'column', colRI)
    return o
end
---Add a schema to the columns table
---@param columnsTbl table
---@param tableName string
---@param schema NetDB.TableSchema
---@param rI number
---@return number
---@nodiscard
local function addSchemaToColumnsTable(columnsTbl, tableName, schema, rI)
    for colName, col in pairs(schema) do
        local row = { ---@type NetDB.ColumnsTableRow
            _rId = rI,
            table = tableName,
            column = colName,
            type = col.type,
            unique = col.unique,
            notNil = col.notNil,
            primaryKey = col.primaryKey,
            def = col.def
        }
        columnsTbl[rI] = row
        rI = rI + 1
    end
    return rI
end
---Crate a new default columns table
---@return table
---@return number
---@nodiscard
local function createDefaultColumnsTable(rI)
    local o = {}
    rI = addSchemaToColumnsTable(o, TABLES_TABLE_NAME, tablesTableSchema, rI)
    rI = addSchemaToColumnsTable(o, COLUMNS_TABLE_NAME, columnsTableSchema, rI)
    return o, rI
end

---@class NetDB.Server.Config
---@field localOnly boolean If the server should be accessible locally only (ie. not over the network)
---@field port number The port the server will use, if open over the network
---@field root string Path to database
---@field userCtrl boolean? If the server should use user control (recommend if network accessible)

---@type NetDB.Server.Config
local defConfig = {
    port = 10031,
    root = '/home/netdb/',
    localOnly = true
}

---@class NetDB.ColumnSchema
---@field type string The data type of the column
---@field primaryKey boolean? If the column in the primary key for the table
---@field unique boolean? If the column's value must be unique in the table
---@field notNil boolean? If the column's value must **not** be nil
---@field def any? The default value of the column, if any

---@class NetDB.TableSchema
---@field [string] NetDB.ColumnSchema Table column, indexed by name

function Server.new()
    local o = {}
    setmetatable(o, ServerMT)
    ---@cast o NetDB.Server
    o:__init__()
    return o
end

function Server:__init__()
    self.__log = pos.Logger('netdb-server', false, true)
    local config = pos.Config('/home/.appdata/netdb/netdb.cfg', defConfig, true)
    self.config = config.data --[[@as NetDB.Server.Config]]
    if (not self.config.localOnly) and (not self.config.userCtrl) then
        self.__log:warn('Server was set to network accessible but user control is disable')
    end
    self:loadIndex()
end

---@param table table
---@param schema NetDB.TableSchema
---@return table outTbl
---@return string primaryKey
---@return number recordIndex
function Server:convertTable(table, schema)
    local pk = nil
    for col, cSchema in pairs(schema) do
        ---@cast cSchema NetDB.ColumnSchema
        if cSchema.primaryKey then
            pk = col
            break
        end
    end
    if not pk then
        for col, cSchema in pairs(schema) do
            ---@cast cSchema NetDB.ColumnSchema
            if cSchema.notNil and cSchema.unique then
                pk = col
                cSchema.primaryKey = true
                break
            end
        end
    end
    if not pk then
        pk = '_rId'
        schema[pk] = { ---@type NetDB.ColumnSchema
            type = 'number',
            unique = true,
            notNil = true,
            primaryKey = true
        }
    end
    local outTbl = {}
    local rIndex = 1
    for i, row in pairs(table) do
        local key
        if row[pk] then
            key = row[pk] .. ''
        else
            key = rIndex .. ''
            row[pk] = rIndex
            rIndex = rIndex + 1
        end
        outTbl[key] = row
    end

    return outTbl, pk, rIndex
end

function Server:loadIndex()
    local indexFN = self.config.root .. 'index.json'

    if not fs.exists(indexFN) then
        self.__log:warn('Could not find index file, creating it')
        local f = fs.open(indexFN, 'w')
        self.__dbs = {
            ['__netdb'] = '__netdb'
        }
        if not f then
            self.__log:error('Error opening index file for writing default')
        else
            f.write(textutils.serialiseJSON({ dbs = self.__dbs }))
            f.close();
        end
        return true
    end

    local f = fs.open(indexFN, 'r')
    if not f then
        self.__log:error('Error opening index file for write')
        return false
    end
    local t = f.readAll()
    f.close();
    local index = textutils.unserialiseJSON(t)
    if not index then
        self.__log:error('Error reading index file: Corrupted')
        return false
    end
    self.__dbs = index.dbs

    for db, _ in pairs(self.__dbs) do
        local path = self.config.root .. db
        if fs.exists(path) then
            if fs.isDir(path) then
                self.__log:error('Malformed database: Found file matching db name `%s` but was not a directory', db)
                return false
            end
        elseif fs.exists(path .. '.db') then -- old version of DB, update it
            self.__log:warn('Found old form DB `%s`: Updating it', db)
            local dbf = fs.open(path .. '.db', 'r')
            if not dbf then
                self.__log:error('Error opening DB `%s` file for updating: %s.db', db, path)
                return false
            end
            local dbText = dbf.readAll()
            dbf.close()
            local dbTable = textutils.unserialiseJSON(dbText)
            if not dbTable then
                self.__log:error('Corrupted DB `%s` file in updating: %s.db', db, path)
                return false
            end
            fs.makeDir(path)

            local columnsTable, colRI = createDefaultColumnsTable(0)
            local tablesTable = createDefaultTablesTable(colRI)

            for tableName, table in pairs(dbTable) do
                if tableName == '_schema' then
                    -- ignore this table, we are already pulling from it for the other tables
                else
                    local schema = dbTable._schema[tableName]
                    local t2, pk, rI = self:convertTable(table, schema)
                    addTableToTablesTable(tablesTable, tableName, pk, rI)
                    colRI = addSchemaToColumnsTable(columnsTable, tableName, schema, colRI)
                    if not self:__saveTable(db, tableName, t2) then
                        return false
                    end
                end
            end
            tablesTable[COLUMNS_TABLE_NAME].recordIndex = colRI
            if not self:__saveTable(db, TABLES_TABLE_NAME, tablesTable) then
                return false
            end
            if not self:__saveTable(db, COLUMNS_TABLE_NAME, columnsTable) then
                return false
            end
        end
    end
end

---Save a table
---@param db string Database name
---@param tableName string Table name
---@param table table Table to save
---@return boolean saved
---@return 'Internal error'? error
function Server:__saveTable(db, tableName, table)
    local f = fs.open(self.config.root .. db .. '/' .. tableName .. '.table', 'w')
    if not f then
        self.__log:error('Error saving table `%s` in database `%s`: Unable to open file for writing', tableName, db)
        return false, 'Internal error'
    end
    f.write(textutils.serialiseJSON(table))
    f.close()
    self.__log:info('Saved table `%s` in database `%s`', tableName, db)
    return true;
end

---Load a table
---@param db string Database name
---@param tableName string Table name
---@return table? table The table *OR* `nil` if the table could not be loaded
function Server:__loadTable(db, tableName)
    local f = fs.open(self.config.root .. db .. '/' .. tableName .. '.table', 'r')
    if not f then
        self.__log:error('Error loading table `%s` in database `%s`: Unable to open file for reading', tableName, db)
        return nil
    end
    local t = f.readAll()
    f.close()
    return textutils.unserializeJSON(t)
end

---Get the schema for a table
---@param db string Database name
---@param tableName string Table name
---@return NetDB.TableSchema? schema Table schema *OR* `nil` if the columns table could not be loaded
---@return ('Internal error')? error
function Server:__getSchema(db, tableName)
    local columns = self:__loadTable(db, COLUMNS_TABLE_NAME)
    if not columns then
        return nil, 'Internal error'
    end
    ---@cast columns {[string]: NetDB.ColumnsTableRow}
    local schema = {}
    for _,col in pairs(columns) do
        if col.table == tableName then
            schema[col.column] = col
        end
    end
    return schema
end

---Get the info about a table from the tables table
---@param db string Database name
---@param tableName string Table name
---@return NetDB.TablesTableRow? info
---@return 'Internal error'|'No such table'? error
function Server:__getTableInfo(db, tableName)
    local tables = self:__loadTable(db, TABLES_TABLE_NAME)
    if not tables then
        return nil, 'Internal error'
    end
    ---@cast tables {[string]: NetDB.TablesTableRow}
    if not tables[tableName] then
        return nil, 'No such table'
    end
    return tables[tableName]
end

---Check if the server has a given table
---@param db string Database name
---@param tableName string Table name
---@return boolean hasTable
function Server:hasTable(db, tableName)
    local i, e = self:__getTableInfo(db, tableName)
    if i then
        return true
    end
    if e ~= 'No such table' then
        error('Internal error checking if table existed', 2)
    end
    return false
end

---Update the record index of a table
---@param db string Database name
---@param tableName string Table name
---@param recordIndex number Record index
---@return boolean
---@return ('Internal error'|'No such table')?
function Server:__saveTableRecordIndex(db, tableName, recordIndex)
    local tables = self:__loadTable(db, TABLES_TABLE_NAME)
    if not tables then
        return false, 'Internal error'
    end
    ---@cast tables {[string]: NetDB.TablesTableRow}
    if not tables[tableName] then
        return false, 'No such table'
    end

    tables[tableName].recordIndex = recordIndex

    return self:__saveTable(db, TABLES_TABLE_NAME, tables)
end

---Verify if a row is valid for the given schema. Will also fill default values
---@param schema NetDB.TableSchema
---@param row table
---@param options {allowPKOverlap: boolean?, allowPartial: boolean?}
---@return boolean valid
---@return string? reason
local function verifyRow(schema, row, table, options)
    local uniqueCols = {}
    for cName, cDef in pairs(schema) do
        ---@cast cDef NetDB.ColumnSchema
        if cName == DEFAULT_RECORD_ID_COLUMN then
            if row[cName] then
                return false, 'Record ID can not be specified'
            end
        else
            if cDef.def and row[cName] == nil then
                row[cName] = cDef.def
            end
            if cDef.notNil and row[cName] == nil then
                if not options.allowPartial then
                    return false, 'Column '..cName..' was specified as NOT_NIL but provided value was nil'
                end
            elseif type(row[cName]) ~= cDef.type then
                return false, 'Column '..cName..' was specified as '..cDef.type..' but provided value was '..type(row[cName])
            end
            if cDef.unique or cDef.primaryKey then
                uniqueCols[cName] = true
            end
            if cDef.primaryKey and options.allowPKOverlap then
                uniqueCols[cName] = nil
            end
        end
    end
    for cName, _ in pairs(row) do
        if not schema[cName] then
            return false, 'Unknown column ' .. cName
        end
    end
    for k,r in pairs(table) do
        for cName, _ in pairs(uniqueCols) do
            if r[cName] == row[cName] then
                return false, 'Duplicate value in column '..cName..' with record '..k
            end
        end
    end
    return true
end

---Add a row to the table. Will overwrite any existing row with the same primary key
---@param db string Database name
---@param tableName string Table name
---@param row table Table row. Will be checked against schema
---@param overwritePK boolean If a row with the same primary key should be overwritten if it exists
---@return boolean added
---@return ('Internal error'|'No such table'|string)? error
function Server:addRow(db, tableName, row, overwritePK)
    local tableInfo, e = self:__getTableInfo(db, tableName)
    if not tableInfo then
        return false, e
    end

    local schema = self:__getSchema(db, tableName)
    if not schema then
        return false, 'Internal error'
    end

    local table = self:__loadTable(db, tableName)
    if not table then
        return false, 'Internal error'
    end

    ---@diagnostic disable-next-line: redefined-local
    local vR, e = verifyRow(schema, row, table, {
        overlapPK = overwritePK
    })
    if not vR then
        return false, e
    end

    local key
    if tableInfo.primaryKey == DEFAULT_RECORD_ID_COLUMN then
        local newId = tableInfo.recordIndex
        local newIndex = newId + 1
        row[DEFAULT_RECORD_ID_COLUMN] = newId
        key = newId .. ''
        self:__saveTableRecordIndex(db, tableName, newIndex)
    else
        key = row[tableInfo.primaryKey] .. ''
    end

    table[key] = row

    return self:__saveTable(db, tableName, table)
end

---Update rows in the database
---@param db string Database name
---@param tableName string Table name
---@param where NetDB.SQLWhere Condition for row update
---@param row table Row data to update
---@return number? updated Number of rows updated
---@return ('Internal error'|'No such table'|string)? error
function Server:updateRows(db, tableName, where, row)
    local tableInfo, e = self:__getTableInfo(db, tableName)
    if not tableInfo then
        return nil, e
    end

    local schema = self:__getSchema(db, tableName)
    if not schema then
        return nil, 'Internal error'
    end

    local table = self:__loadTable(db, tableName)
    if not table then
        return nil, 'Internal error'
    end

    ---@diagnostic disable-next-line: redefined-local
    local vR, e = verifyRow(schema, row, table, {
        overlapPK = true,
        allowPartial = true
    })
    if not vR then
        return nil, e
    end

    if #where.conditions == 1 then -- if it is only using an equals of IN on the primary key, just do it directly
        local cond = where.conditions[0] ---@type NetDB.SQLWhere.Condition
        if cond.col == tableInfo.primaryKey and (not cond.invert) then -- might be able to optimize the update here
            if cond.op == '=' then
                local key = cond.val[0]
                if table[key] then
                    for c, v in pairs(row) do
                        table[key][c] = v
                    end
                    ---@diagnostic disable-next-line: redefined-local
                    local s, e = self:__saveTable(db, tableName, table)
                    if not s then
                        return nil, e
                    end
                    return 1
                else
                    return 0
                end
            elseif cond.op == 'IN' then
                local rowsChanged
                for _, key in pairs(cond.val) do
                    if table[key] then
                        for c, v in pairs(row) do
                            table[key][c] = v
                        end
                        rowsChanged = rowsChanged + 1
                    end
                end

                if rowsChanged > 0 then
                    ---@diagnostic disable-next-line: redefined-local
                    local s, e = self:__saveTable(db, tableName, table)
                    if not s then
                        return nil, e
                    end
                end

                return rowsChanged
            end
        end
    end

    local rowsChanged
    for key, r2 in pairs(table) do
        if where:check(r2) then
            for c, v in pairs(row) do
                table[key][c] = v
            end
            rowsChanged = rowsChanged + 1
        end
    end
    
    if rowsChanged > 0 then
        ---@diagnostic disable-next-line: redefined-local
        local s, e = self:__saveTable(db, tableName, table)
        if not s then
            return nil, e
        end
    end

    return rowsChanged
end

---Get rows from the database
---@param db string Database name
---@param tableName string Table name
---@param where NetDB.SQLWhere? Condition for row selection (`nil` for all rows)
---@param columns string[]? Columns to get (`nil` for all columns)
---@return table[]? rows
---@return ('Internal error'|'No such table'|string)? error
function Server:getRows(db, tableName, where, columns)
    local tableInfo, e = self:__getTableInfo(db, tableName)
    if not tableInfo then
        return nil, e
    end

    local schema = self:__getSchema(db, tableName)
    if not schema then
        return nil, 'Internal error'
    end

    local table = self:__loadTable(db, tableName)
    if not table then
        return nil, 'Internal error'
    end

    if not where then
        local rows = {}
        for _, r in pairs(table) do
            local outRow = {}
            if columns then
                for _, c in pairs(columns) do
                    outRow[c] = r[c]
                end
            else
                outRow = r
            end
            table.insert(rows, outRow)
        end
        return rows
    end
    
    if #where.conditions == 1 then                                     -- if it is only using an equals of IN on the primary key, just do it directly
        local cond = where.conditions[0] ---@type NetDB.SQLWhere.Condition
        if cond.col == tableInfo.primaryKey and (not cond.invert) then -- might be able to optimize the update here
            if cond.op == '=' then
                local key = cond.val[0]
                if table[key] then
                    local outRow = {}
                    if columns then
                        for _, c in pairs(columns) do
                            outRow[c] = table[key][c]
                        end
                    else
                        outRow = table[key]
                    end

                    return { outRow }
                else
                    return {}
                end
            elseif cond.op == 'IN' then
                local rows = {}
                for _, key in pairs(cond.val) do
                    if table[key] then
                        local outRow = {}
                        if columns then
                            for _, c in pairs(columns) do
                                outRow[c] = table[key][c]
                            end
                        else
                            outRow = table[key]
                        end
                        table.insert(rows, outRow)
                    end
                end

                return rows
            end
        end
    end

    local rows = {}
    for _, r in pairs(table) do
        if where:check(r) then
            local outRow = {}
            if columns then
                for _, c in pairs(columns) do
                    outRow[c] = r[c]
                end
            else
                outRow = r
            end
            table.insert(rows, outRow)
        end
    end
    
    return rows
end

local Tokenizer = dofile('Tokenizer.lua') ---@type NetDB.Tokenizer

function Server:executeSQL(db, commands)
    local tokens = Tokenizer.tokenize(commands)

    local cmds = {
        [1] = {}
    }
    for _, token in pairs(tokens) do
        if type(token) == "table" and token[';'] == true then
            table.insert(cmds, {})
        else
            table.insert(cmds[#cmds], token)
        end
    end
    
    if #cmds == 1 then
        return self:__executeSingle(db, cmds[1])
    end
    local o = {}
    for i,cmd in pairs(cmds) do
        o[i] = self:__executeSingle(db, cmd)
    end
    return o
end

local SQLWhere = dofile('SQLWhere.lua') ---@type NetDB.SQLWhere

---@param tokens (boolean|string|number)[]
function Server:__executeSingle(db, tokens)
    local action = tokens[1]:lower()

    if action == 'show' then
        local a2 = tokens[2]:lower()
        if a2 == 'database' then
            local dbs = {}
            for n, _ in pairs(self.__dbs) do
                table.insert(dbs, n)
            end
            return dbs
        elseif a2 == 'tables' then
            local tables = self:__loadTable(db, TABLES_TABLE_NAME)
            if not table then
                return 'Internal error'
            end
            ---@cast tables NetDB.TablesTable
            local out = {}
            for n, _ in pairs(tables) do
                table.insert(out, n)
            end
            return out
        elseif a2 == 'schema' then
            local tblName = tokens[3] --[[@as string]]
            local schema, e = self:__getSchema(db, tblName)
            if not schema then
                return e
            end
            return schema
        end
    elseif action == 'insert' then
        if not tokens[2]:lower() == 'into' then
            return ('Malformed command: @ `%s^%s`...; INTO expected'):format(tokens[1] .. '', tokens[2] .. '')
        end

        local tblName = tokens[3] --[[@as string]]
        local cols = { tokens[4] }
        local row = nil ---@type table
        local vs = 0
        local i = 5
        local lastEl = true
        while i <= #tokens and vs <= # cols do
            local token = tokens[i]
            if lastEl then
                if not row and token:lower() == 'values' then
                    row = {}
                elseif type(token) == "table" and token[','] then
                    lastEl = false
                else
                    return ('Malformed command: @ ...`%s^%s`...; VALUES or `,` expected'):format(tokens[i - 1] .. '',
                        tokens[i] .. '')
                end
            else
                if not row then -- column
                    table.insert(cols, token)
                elseif row then -- value
                    row[cols[vs]] = token
                    vs = vs + 1
                end
                lastEl = true
            end
            i = i + 1
        end
        if i <= #tokens then
            return ('Malformed command: @ ...`%s^%s`...; End of command expected'):format(tokens[i - 1] .. '',
                tokens[i] .. '')
        end
        local s, e = self:addRow(db, tblName, row, false)
        if not s then
            return e
        end
        return 'Inserted row into `' .. tblName .. '`'
    elseif action == 'select' then
        local cols = { tokens[2] }
        local lastCol = true
        local moreCols = true
        local i = 3
        while i <= #tokens and moreCols do
            local token = tokens[i]
            if lastCol and (type(token) == 'table' and token[',']) then
                lastCol = false
            elseif not lastCol then
                table.insert(cols, token)
            elseif token:lower() == 'from' then
                moreCols = false
            else
                return ('Malformed command: @ ...`%s^%s`...; FROM or `,` expected'):format(tokens[i - 1] .. '',
                    tokens[i] .. '')
            end
            i = i + 1
        end
        if #cols == 1 and cols[1] == '*' then
            cols = nil
        end

        local tableName = tokens[i] --[[@as string]]

        local where = nil;
        if #tokens > i then
            i = i + 1
            if tokens[i]:lower() == 'where' then
                local w, e = SQLWhere.parse(tokens, i + 1)
                if not w then
                    return e
                end
                where = w
            else
                return ('Malformed command: @ ...`%s^%s`...; WHERE or command end expected'):format(tokens[i - 1] .. '',
                    tokens[i] .. '')
            end
        end

        local rs, e = self:getRows(db, tableName, where, cols)
        if not rs then
            return e
        end
        return rs
    elseif action == 'update' then
        
    end
    
    return ('Malformed command: @ `^%s`...; Unknown action'):format(tokens[1])
end

return Server;