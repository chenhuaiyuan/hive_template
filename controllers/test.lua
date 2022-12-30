local _M = {}
local valid = require 'utils.validation'

function _M.index(request)
  return {
    ['status'] = 200,
    ['headers'] = {
      ['Content-type'] = 'text/html'
    },
    ['body'] = '<h1>hello world!</h1>'
  }
end

function _M.get_user_info(request)
  local params = request._request:params()
  valid.require(params, { 'username', 'age' })
  valid.number(params, { 'age' })
  local user = { username = params.username, age = params.age }
  return _RESPONSE.success(user)
end

function _M.test(request)
  return "hello world"
end

return _M
