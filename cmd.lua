netdb.setup()

local args = { ... }

_G.cmd = _G.cmd or {}
cmd.netdb = cmd.netdb or {}
cmd.netdb.database = cmd.netdb.database or ''

local db = cmd.netdb.database

if args[1] then
    args[1] = string.lower(args[1])
end

if args[1] == 'host' then
    netdb.server.start()
    print('Starting NetBD server')
    return
end

if args[1] == 'list' then
    if args[2] == 'tables' then
        if cmd.netdb.database == '' then
            print('Slect a database first')
            return
        end
        local r, tbls = netdb.server.listTables(db)
        if not r then
            print('Failure: ' .. tbls)
            return
        end
        print('Listing tables in `' .. db .. '`')
        for _, tbl in pairs(tbls) do
            print('`'..tbl..'`')
        end
    else
        print('Listing databases')
        local r, dbs = netdb.server.listDbs()
        for _, db in pairs(dbs) do
            print('`'..db..'`')
        end
    end
    return
elseif args[1] == 'create' then
    if args[2] == 'database' then
        local s, r = netdb.server.createDatabase(args[3])
        print(r)
    elseif args[2] == 'table' then
        if cmd.netdb.database == '' then
            print('Slect a database first')
            return
        end
        local s, r = netdb.server.createTable(cmd.netdb.database, args[3])
        print(r)
    else
        print('Must specify table or db: netdb create [database|table] [name]')
    end
    return
elseif args[1] == 'using' then
    if netdb.server.hasDb(args[2]) then
        cmd.netdb.database = args[2]
        print('Now using database `' .. args[2] .. '`')
    else
        print('Database does not exist')
    end
    return
end

if #args == 0 then
    -- start command session
    print('Starting NetDB command session:')
    write('> ')
    local cmd = read()
    while cmd ~= 'quit' do
        if string.start(cmd, 'using') then
            local pts = string.split(cmd, ' ')
            local tdb = table.concat(pts, ' ', 2)
            if netdb.server.hasDb(tdb) then
                db = tdb
                print('Using database `' .. db .. '`')
            else
                print('Database does not exist')
            end
        elseif db ~= '' then
            local s, r = netdb.server.run(db, cmd)
            if not s then
                print('ERROR: ' .. r)
            elseif type(r) == 'table' then
                print(textutils.serialise(r))
            else
                print(r)
            end
        else
            print('Select a database first')
        end
        print()
        write('> ')
        cmd = read()
    end
    return
end
if args[1] == 'remote' then
    local con = netdb.open(args[2], args[3])
    if not con then
        print('Could not open connection')
        return
    end
    if #args == 5 then
        con:setCredentials(args[4], args[5])
    end
    print('Starting NetDB command session:')
    write('> ')
    local cmd = read()
    while cmd ~= 'quit' do
        local s, r = con:run(cmd)
        if not s then
            print('ERROR: ' .. r)
        elseif type(r) == 'table' then
            print(textutils.serialise(r))
        else
            print(r)
        end
        print()
        write('> ')
        cmd = read()
    end
    return
end

if cmd.netdb.database == '' then
    print('Select a database first')
    return
end

if args[1] == 'select' then
    local i = 2
    local cols
    if args[i] ~= '*' then
        cols = {}
        while string.cont(args[i], ',') and i < #args do
            table.insert(cols, string.sub(args[i], 1, -2))
            i = i + 1
        end
        table.insert(cols, args[i])
    else
        i = i + 1
    end
    i = i + 1
    local tbl = args[i]
    i = i + 2
    local sCols
    local sVals
    if i < #args then
        sCols = {}
        sVals = {}
        local c, e, v = true, false, false
        while i <= #args do
            if c then
                table.insert(sCols, args[i])
                c = false
                e = true
            elseif e then
                e = false
                v = true
            elseif v then
                local val = args[i]
                if string.cont(val, ',') then
                    val = string.sub(val, 1, -2)
                end
                table.insert(sVals, val)
                v = false
                c = true
            end
            i = i + 1
        end
    end
    local s, r = netdb.server.get(db, tbl, sCols, sVals, cols)
    if not s then
        print('Error: ' .. r)
        return
    end
    print('Return:')
    for _, row in pairs(r) do
        print(netdb.printTbl(row))
    end
    return
elseif args[1] == 'insert' then
    local tbl = args[3]
    local i = 4
    local cols = {}
    while string.cont(args[i], ',') and i <= #args do
        table.insert(cols, string.sub(args[i], 1, -2))
        i = i + 1
    end
    table.insert(cols, args[i])

    i = i + 2

    local vals = {}
    while string.cont(args[i], ',') and i <= #args do
        table.insert(vals, string.sub(args[i], 1, -2))
        i = i + 1
    end
    table.insert(vals, args[i])

    local s, r = netdb.server.insert(db, tbl, cols, vals)
    if not s then
        print('Error: ' .. r)
        return
    end
    print('Inserted row')
    return
end

print('Unknown command')