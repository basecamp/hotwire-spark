class PulseWire::ActionCable::PersistentCableMiddleware
  def initialize(app)
    @app = app
  end

  def call(env)
    request = Rack::Request.new(env)

    status, headers, response = if request.params["pulse_wire"]
      ActionCable.server.without_restarting { @app.call(env) }
    else
      @app.call(env)
    end

    [status, headers, response]
  end
end