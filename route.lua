-- local auth_token = require 'middleware.auth_token'
local test = require 'controllers.test'

local router = hive.router().new()


router:match('GET', '/', test.index)
-- match第四个参数是传中间件函数
-- router:match('GET', '/test', test.index, auth_token)
router:match('GET', '/get_user_info', test.get_user_info)
router:match('GET', '/test', test.test)
router:match('GET', '/template', test.template)

return router
