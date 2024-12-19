require "action_cable/server/base"

module Hotwire::Spark
  class Engine < ::Rails::Engine
    isolate_namespace Hotwire::Spark

    config.hotwire = ActiveSupport::OrderedOptions.new unless config.respond_to?(:hotwire)
    config.hotwire.spark = ActiveSupport::OrderedOptions.new
    config.hotwire.spark.merge! \
      enabled: Rails.env.development?,
      css_paths: File.directory?("app/assets/builds") ? %w[ app/assets/builds ] : %w[ app/assets/stylesheets ],
      html_paths: %w[ app/controllers app/helpers app/models app/views ],
      stimulus_paths: %w[ app/javascript/controllers ]

    initializer "hotwire_spark.config" do |app|
      config.hotwire.spark.each do |key, value|
        Hotwire::Spark.send("#{key}=", value)
      end
    end

    initializer "hotwire_spark.assets" do |app|
      app.config.assets.paths << root.join("app/assets/javascripts")
    end

    initializer "hotwire_spark.install" do |application|
      Hotwire::Spark.install_into application if Hotwire::Spark.enabled?
    end
  end
end
