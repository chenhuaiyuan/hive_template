local _M = {}

function _M.index()
  return {
    ['status'] = 200,
    ['headers'] = {
      ['Content-type'] = 'text/html'
    },
    ['body'] = '<h1>hello world!</h1>'
  }
end

return _M
