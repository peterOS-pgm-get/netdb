local PARTNER_CHARACTERS = {
    ['\''] = true,
    ['\"'] = true,
    ['`'] = true
}
local CONDITION_CHARACTERS = {
    ['='] = {},
    ['<'] = {['='] = true},
    ['>'] = {['='] = true},
    ['!'] = {['='] = true}
}
local SPECIAL_CHARACTERS = {
    [';'] = true,
    [','] = true
}

---@class NetDB.Tokenizer
---@field tokens (string|number|boolean)[]
---@field workingToken string?
---@field workingCondition {[string]: true}?
---@field partnerCharacter string?
---@field escaped boolean
local Tokenizer = {}

---@package
function Tokenizer:__init__()
    self.tokens = {}
    self.escaped = false
end

---Tokenize the provided text
---@param text string
---@return (string|number|boolean)[] tokens
function Tokenizer.tokenize(text)
    local tokenizer = {}
    setmetatable(tokenizer, { __index = Tokenizer })
    ---@cast tokenizer NetDB.Tokenizer
    tokenizer:__init__()
    for i = 1, #text do
        local c = text:sub(i,i)
        tokenizer:__processCharacter(c)
    end
    if tokenizer.workingToken then
        tokenizer:__addWorkingToken()
    end
    return tokenizer.tokens
end

---@private
function Tokenizer:__addWorkingToken()
    local nV = tonumber(self.workingToken)
    local t = self.workingToken
    ---@cast t +number|boolean -?
    if nV then
        ---@diagnostic disable-next-line: cast-local-type
        t = nV
    elseif self.workingToken == 'true' then
        ---@diagnostic disable-next-line: cast-local-type
        t = true
    elseif self.workingToken == 'false' then
        ---@diagnostic disable-next-line: cast-local-type
        t = false
    end
    print('Adding token '..t)
    table.insert(self.tokens, t)
    self.workingToken = nil
end

---@private
function Tokenizer:__processCharacter(c)
    if self.partnerCharacter then -- in quotes or similar
        if not self.escaped then  -- not preceded by a '\'
            if c == '\\' then
                self.escaped = true
                return
            elseif c == self.partnerCharacter then -- it's the character we are looking for, and it's not escaped
                print('Adding token '..self.partnerCharacter .. self.workingToken..self.partnerCharacter)
                self.partnerCharacter = nil
                table.insert(self.tokens, self.workingToken)
                self.workingToken = nil
                return
            end
        end
        if not self.workingToken then
            self.workingToken = c
        else
            self.workingToken = self.workingToken .. c
        end
        return
    end -- not quoted

    if self.workingCondition then
        if self.workingCondition[c] then -- if this character can be in
            self.workingToken = self.workingCondition .. c
            self.workingCondition = {}
            return
        end
        print('Adding condition '..self.workingToken)
        table.insert(self.tokens, self.workingToken)
        self.workingCondition = nil
        self.workingToken = nil -- don't continue here, could need 
    end
    
    if not self.workingToken and PARTNER_CHARACTERS[c] then -- entering quotes
        self.partnerCharacter = c
        return
    elseif self.workingToken and c == ' ' then              -- word boundary (and we are working on a token)
        self:__addWorkingToken()
        return
    elseif CONDITION_CHARACTERS[c] then -- start a condition (could be directly against a column)
        if self.workingToken then
            self:__addWorkingToken()
        end
        self.workingCondition = CONDITION_CHARACTERS[c]
        self.workingToken = c
        return
    end
    
    if not self.workingToken then -- no token started
        if c == ' ' then -- don't start a token on a word boundary
            return
        elseif SPECIAL_CHARACTERS[c] then -- don't make one for command separator
            print('Special character '..c)
            table.insert(self.tokens, {[c] = true})
            return
        end
        self.workingToken = c
    else -- add it the current token
    if SPECIAL_CHARACTERS[c] then
            self:__addWorkingToken()
            print('Special character '..c)
            table.insert(self.tokens, {[c] = true})
            return
        end
        self.workingToken = self.workingToken .. c
    end
end

return Tokenizer