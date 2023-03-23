_RESPONSE = require 'response'
local exception = dofile 'exception/base_exception.lua'
require 'config'
local query = loadfile 'orm/query.lua' -- 支持mysql和sqlite
-- local mongo = require 'mongo'
local router = require 'route'

-- query().new(MYSQL_USER, MYSQL_PASS, MYSQL_HOST, DATABASE)
-- query().open('./data/test.db3') -- 需要安装sqlite扩展， 扩展在ext目录下
-- local client = mongo.Client(MONGO)
-- Mongo = client:getDatabase(DATABASE)


local function exec(method, path, req)
  -- local remote_addr = req:remote_addr() -- 获取客户端地址
  -- local headers = req:headers() -- 获取头部信息
  -- local params = { _request = req, _remote_addr = remote_addr, _headers = headers }
  local params = { _request = req }
  local handler = router:execute(method, path)
  if handler.is_exist then
    params.router_params = handler.router_params
    if handler.middleware ~= nil then
      local is_pass = false;
      is_pass, params._user_info = handler.middleware(req)
      if not is_pass then
        -- local res = { code = 5001, message = 'Failed to verify token', data = '' }
        -- return {
        --   ['status'] = 200,
        --   ['headers'] = {
        --     ['Content-type'] = 'application/json'
        --   },
        --   ['body'] = hive.to_json(res)
        -- }
        return _RESPONSE.fail(5001, 'Failed to verify token')
      end
    end
    return handler.func(params)
  else
    return hive.response.new():status(404):headers({
      ['Content-type'] = 'application/json'
    }):body({
      ['code'] = 404,
      ['data'] = '',
      ['message'] = 'Not Found'
    })
    -- return {
    --   ['status'] = 404,
    --   ['headers'] = {
    --     ['Content-type'] = 'application/json'
    --   },
    --   ['body'] = hive.to_json({
    --     ['code'] = 404,
    --     ['data'] = '',
    --     ['message'] = 'not found'
    --   })
    -- }
  end
end

-- return exec, exception
local s = hive.server():bind("127.0.0.1", 3000):exception(exception):serve(exec)
return s:run()
