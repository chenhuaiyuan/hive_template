local auth_token = require 'middleware.auth_token'
local test = require 'controllers.test'

local router = hive.router().new()


router:match('get', '/test', test.index)
-- match第四个参数是传中间件函数
-- router:match('GET', '/test', test.index, auth_token)

return router
