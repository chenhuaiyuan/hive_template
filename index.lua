-- require 'config'
-- local mongo = require 'mongo'

local mysql = loadfile 'orm/mysql.lua'
local router = require 'route'


-- mysql().new(MYSQL_USER, MYSQL_PASS, MYSQL_HOST)

-- local client = mongo.Client(MONGO)
-- Mongo = client:getDatabase(DATABASE)


local function exec(method, path, req)
  local remote_addr = req:remote_addr()
  local headers = req:headers()
  local bool, resp, middleware, params = router:execute(method, path,
    { _request = req, _remoteAddr = remote_addr, _headers = headers })
  if bool then
    if middleware then
      local is_pass = false;
      is_pass, params._user_info = middleware(req)
      if not is_pass then
        return _RESPONSE.fail(5001, 'Failed to verify token')
      end
    end
    return resp(params)
  else
    return {
      ["status"] = 404,
      ["headers"] = {
        ["Content-type"] = "application/json"
      },
      ["body"] = hive.table_to_json({
        ['code'] = 404,
        ['data'] = '',
        ['message'] = 'not found'
      })
    }
  end
end

return exec
