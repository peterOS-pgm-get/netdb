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
        local r, tables = netdb.server.listTables(db)
        if not r then
            print('Failure: ' .. tables)
            return
        end
        print('Listing tables in `' .. db .. '`')
        ---@cast tables table
        for _, tbl in pairs(tables) do
            print('`'..tbl..'`')
        end
    else
        print('Listing databases')
        local r, dbs = netdb.server.listDbs()
        for _, database in pairs(dbs) do
            print('`'..database..'`')
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
        print('Must specify table or db: netdb create <database|table> [name]')
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

print('Unknown command')