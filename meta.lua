---@meta

---@class NetDB.Message : NetMessage
---@field header NetDB.Message.Header
---@field body NetDB.Message.Body

---@class NetDB.Message.Header : NetMessage.Header
---@field method string
---@field db string

---@class NetDB.Message.Body
---@field user NetDB.Message.Body.User
---@field table string
---@field cmd string?

---@class NetDB.Message.Body.User
---@field name string
---@field password string

---@class NetDB.Message.GetBody : NetDB.Message.Body
---@field sel { cols: string[], vals: string[] }
---@field cols string[]

---@class NetDB.Message.PutBody : NetDB.Message.Body
---@field sel { cols: string[], vals: string[] }
---@field data { cols: string[], vals: string[] }

---@class NetDB.Message.InsertBody : NetDB.Message.Body
---@field cols string[]
---@field vals any[]

---@class NetDB.Message.ExistsBody : NetDB.Message.Body
---@field cols string[]
---@field vals any[]