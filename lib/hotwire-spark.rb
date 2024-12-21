require "hotwire/spark/version"
require "hotwire/spark/engine"

require "zeitwerk"
loader = Zeitwerk::Loader.for_gem(warn_on_extra_files: false)
loader.ignore("#{__dir__}/hotwire-spark.rb")
loader.ignore("#{__dir__}/hotwire/spark/version.rb")
loader.setup

module Hotwire::Spark
  mattr_accessor :css_paths, default: []
  mattr_accessor :html_paths, default: []
  mattr_accessor :stimulus_paths, default: []
  mattr_accessor :logging, default: false

  mattr_accessor :enabled, default: Rails.env.development?

  class << self
    def install_into(application)
      Installer.new(application).install
    end

    def enabled?
      enabled && defined?(Rails::Server)
    end

    def cable_server
      @server ||= Hotwire::Spark::ActionCable::Server.new(config: ::ActionCable::Server::Base.config.dup).tap do |server|
        server.config.connection_class = -> { ::ActionCable::Connection::Base }
      end
    end
  end
end
