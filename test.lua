box.cfg{
    listen = '127.0.0.1:10001',
}

box.schema.user.grant('guest', 'read,write,execute', 'universe')

function test2(a, b)
    return b, a, "test"
end

function test3(int, uint, string, array_of_int)
    local a2 = {int, uint}
    local a3 = {string, string}
    return array_of_int, a2, a3, "test3"
end
