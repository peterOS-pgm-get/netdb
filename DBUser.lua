---@class DBUser
---@field name string User name
---@field password string Password hash (SHA256)
---@field access { [string]: boolean} Databases the user can access
---@field origin { [string]: boolean} Where the user can connect from
---@field perms { [string]: boolean} Method permissions
local DBUser = {}
---@diagnostic disable-next-line: missing-fields
local DefaultDBUser = { ---@type DBUser
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
    setmetatable(userOut, { __index = DBUser })
    if user.access ~= '*' and type(user.access) == 'string' then
        local t = user.access:split(',')
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
        local t = user.perms:split(',')
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
        local t = user.origin:split(',')
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

return DBUser, DefaultDBUser