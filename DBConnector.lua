---@class DBConnector
---@field server string|integer Database server address
---@field db string Database name,
---@field port number Database server port,
---@field _user DBUserCredentials|nil User credentials for
---@field __log Logger NetDB logger
local DBConnector = {
    user = nil
}

---@class DBUserCredentials
---@field name string Username
---@field password string plaintext password

---Create a new DB connector
---@param server string|integer Database server address
---@param db string Database name
---@param port integer Database port
---@param log Logger Logger for the connector
---@return DBConnector connector
function _G.netdb.createConnector(server, db, port, log)
    local o = {
        server = server,
        db = db,
        port = port,
        __log = log
    }
    setmetatable(o, { __index = DBConnector } ) ---@cast o DBConnector
    return o
end

---Ping remote database to check connectionIds
---@return boolean successful
function DBConnector:ping()
    if self.server ~= 'localhost' then
        local rsp = net.sendAdvSync(self.port, self.server,
            { type = 'netdb', method = 'ping' },
            {}
        )
        --[[
        local rsp = net.sendAdvSync(self.port, self.server, { type = 'netdb', method = 'ping' }, {} )
        ]]
        if type(rsp) == 'string' then
            self.__log:error('Ping failed: Network fail: ' .. rsp)
            return false
        end
    end
    return true
end

---Set user credentials
---@param name string username
---@param password string password
function DBConnector:setCredentials(name, password)
    self._user = {
        name = name,
        password = password
    }
end

---Get data from database
---@param table string table name
---@param sCols string[] list of columns to check
---@param sVals any[] list of values to check against
---@param cols string[] list of columns to get
---@return boolean success
---@return string|table r Selected rows OR error message
function DBConnector:get(table, sCols, sVals, cols)
    if self.server == 'localhost' then
        local s, r = netdb.server.get(self.db, table, sCols, sVals, cols)
        return s, r
    end
    local rsp = net.sendAdvSync(self.port, self.server,
        { type = 'netdb', method = 'get', db = self.db },
        { table = table, sel = { cols = sCols, vals = sVals }, cols = cols, user = self._user }
    )
    if type(rsp) == 'string' then
        self.__log:error('Get failed: Network fail: ' .. rsp)
        return false, rsp
    end
    if not rsp.header.suc then
        self.__log:error('Get failed: ' .. rsp.body.error)
        return false, rsp.body.error
    else
        return true, rsp.body
    end
end

---Put data into the database
---@param table string table name
---@param sCols string[] list of selector columns
---@param sVals any[] list of selector values
---@param dCols string[] list of set columns
---@param dVals any[] list of set values
---@return boolean success
---@return boolean|string r Put return OR error message
function DBConnector:put(table, sCols, sVals, dCols, dVals)
    if self.server == 'localhost' then
        local s, r = netdb.server.put(self.db, table, sCols, sVals, dCols, dVals)
        return s, r
    end
    local rsp = net.sendAdvSync(self.port, self.server,
        { type = 'netdb', method = 'put', db = self.db },
        {
            table = table,
            sel = { cols = sCols, vals = sVals },
            data = { cols = dCols, vals = dVals },
            user = self._user
        }
    )
    if type(rsp) == 'string' then
        self.__log:error('Put failed: Network fail: ' .. rsp)
        return false, rsp
    end
    if not rsp.header.suc then
        self.__log:error('Put failed: ' .. rsp.body.error)
        return false, rsp.body.error
    else
        return true, rsp.body[1]
    end
end

---Insert a new row
---@param table string table name
---@param cols string[] data column names
---@param vals any[] data column values
---@return boolean success
---@return boolean|string r Insert valid OR error message
function DBConnector:insert(table, cols, vals)
    if self.server == 'localhost' then
        local s, r = netdb.server.insert(self.db, table, cols, vals)
        return s, r
    end
    local rsp = net.sendAdvSync(self.port, self.server,
        { type = 'netdb', method = 'insert', db = self.db },
        { table = table, cols = cols, vals = vals, user = self._user }
    )
    if type(rsp) == 'string' then
        self.__log:error('Insert failed: Network fail: ' .. rsp)
        return false, rsp
    end
    if not rsp.header.suc then
        self.__log:error('Insert failed: ' .. rsp.body.error)
        return false, rsp.body.error
    else
        return true, rsp.body[1]
    end
end

---Check if a row exits with selector
---@param table string table name
---@param cols string[] selector column names
---@param vals any[] selector column values
---@return boolean success
---@return boolean|string r Row exists OR error message
function DBConnector:exists(table, cols, vals)
    if self.server == 'localhost' then
        local s, r = netdb.server.exists(self.db, table, cols, vals)
        return s, r
    end
    local rsp = net.sendAdvSync(self.port, self.server,
        { type = 'netdb', method = 'exists', db = self.db },
        { table = table, sel = { cols = cols, vals = vals }, user = self._user }
    )
    if type(rsp) == 'string' then
        self.__log:error('Exists failed: Network fail: ' .. rsp)
        return false, rsp
    end
    if not rsp.header.suc then
        self.__log:error('Exists failed: ' .. rsp.body.error)
        return false, rsp.body.error
    else
        return true, rsp.body[1]
    end
end

---Run an SQL style command
---@param command string commands
---@return boolean success
---@return string|table r Command return OR error message
function DBConnector:run(command)
    if self.server == 'localhost' then
        local s, r = netdb.server.run(self.db, command)
        if not s then
            self.__log:error('Run failed: ' .. r)
            self.__log:debug('Command dump: ' .. command)
        end
        return s, r
    end
    local rsp = net.sendAdvSync(self.port, self.server,
        { type = 'netdb', method = 'run', db = self.db },
        { cmd = command, user = self._user }
    )
    if type(rsp) == 'string' then
        self.__log:error('Run failed: Network fail: ' .. rsp)
        return false, rsp
    end
    if not rsp.header.suc then
        self.__log:error('Run failed: ' .. rsp.body.error)
        self.__log:debug('Command dump: ' .. command)
        return false, rsp.body.error
    else
        return true, rsp.body
    end
end