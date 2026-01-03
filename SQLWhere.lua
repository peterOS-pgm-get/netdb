---@class NetDB.SQLWhere
---@field conditions (NetDB.SQLWhere.Condition|'AND'|'OR')[]
local SQLWhere = {}
local SQLWhereMT = {
    __index = SQLWhere
}

---@class NetDB.SQLWhere.Condition
---@field invert boolean?
---@field col string
---@field check fun(self: table, value: any): boolean
---@field val any[]
---@field op '='|'!='|'<'|'<='|'>'|'>='|'BETWEEN'|'IN'|'LIKE'

---@return NetDB.SQLWhere.Condition
local function makeCondition()
    return {
        col = nil,
        check = nil,
        val = nil,
        op = nil
    }
end

function SQLWhere:__init__()
    self.conditions = {}
end

local comparisonFunctions = {
    ['='] = function(self, v) return self.val[1] == v end,
    ['!='] = function(self, v) return self.val[1] ~= v end,
    ['<'] = function(self, v) return self.val[1] < v end,
    ['<='] = function(self, v) return self.val[1] <= v end,
    ['>'] = function(self, v) return self.val[1] > v end,
    ['>='] = function(self, v) return self.val[1] >= v end,
    ['BETWEEN'] = function(self, v) return self.val[1] <= v and v <= self.val[2] end,
    ['IN'] = function(self, v)
        for _, v2 in pairs(self.val) do
            if v == v2 then
                return true
            end
        end
        return false
    end,
    ['LIKE'] = function(self, v)
        return string.find(v, self.val[1])
    end
}

---comment
---@param tokens (string|number|boolean|table)[]
---@param sI any
---@return NetDB.SQLWhere? where
---@return string? error
function SQLWhere.parse(tokens, sI)
    local o = {}
    setmetatable(o, SQLWhereMT)
    ---@cast o NetDB.SQLWhere
    o:__init__()

    local nextCond = nil ---@type NetDB.SQLWhere.Condition?

    if #tokens <= sI then
        return nil
    end

    local isCondValue = false

    for i=sI, #tokens do
        local token = tokens[i]

        if nextCond and nextCond.check then
            if nextCond.val then
                if type(token) == "table" and token[','] then -- comma, so keep going
                    isCondValue = true
                elseif isCondValue then
                    table.insert(nextCond.val, token)
                    isCondValue = false
                else -- end of value
                    o:_addCondition(nextCond)
                    nextCond = nil
                end
            else
                nextCond.val = { token }
            end
        end

        if not nextCond then
            if token == 'AND' or token == 'OR' then
                if #o.conditions == 0 then
                    return nil, 'Boolean operations can not precede all conditions'
                elseif type(o.conditions[#o.conditions]) == "string" then
                    return nil, 'Boolean operations must go between two condition statements'
                end
                o:_addCondition(token)
            elseif token == 'NOT' then
                if #o.conditions > 0 and type(o.conditions[#o.conditions]) ~= "string" then
                    return nil, 'Conditions must be combined will AND or OR'
                end
                nextCond = makeCondition()
                nextCond.invert = true
            else
                if #o.conditions > 0 and type(o.conditions[#o.conditions]) ~= "string" then
                    return nil, 'Conditions must be combined will AND or OR'
                end
                nextCond = makeCondition()
                nextCond.col = token --[[@as string]]
            end
        elseif not nextCond.check then -- column set
            nextCond.check = comparisonFunctions[token]
            nextCond.op = token --[[@as string]]
            if not nextCond.check then
                return nil, 'Unknown comparison: `' .. token .. '`'
            end
            isCondValue = true
        else
            -- pass here cause we already did it
        end
    end

    return o
end

---comment
---@param condition NetDB.SQLWhere.Condition|'AND'|'OR'
function SQLWhere:_addCondition(condition)
    table.insert(self.conditions, condition)
end

function SQLWhere:check(row)
    local v = false
    local nOp = nil
    for i = 1, #self.conditions do
        local condition = self.conditions[i]
        if type(condition) == "string" then
            nOp = condition
        else
            local v2 = condition:check(row[condition.col])
            if condition.invert then
                v2 = not v2
            end
            if nOp == 'AND' then
                v = v and v2
            elseif nOp == 'OR' then
                v = v or v2
            else
                v = v
            end
        end
    end
    return v
end

return SQLWhere