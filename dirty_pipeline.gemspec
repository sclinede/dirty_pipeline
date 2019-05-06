
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "dirty_pipeline/version"

Gem::Specification.new do |spec|
  spec.name          = "dirty_pipeline"
  spec.version       = DirtyPipeline::VERSION
  spec.authors       = ["Sergey Dolganov"]
  spec.email         = ["sclinede@gmail.com"]

  spec.summary       = %q{Simple state machine designed for non-pure transitions}
  spec.description   = %q{Simple state machine designed for non-pure transitions. E.g. for wizard-like systems with a lot of external API calls.}
  spec.homepage      = "https://github.com/sclinede/dirty_pipeline"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # temporary dependency
  spec.add_runtime_dependency "sidekiq", "~> 5.0"
  spec.add_runtime_dependency "redis", "~> 4.0"
  spec.add_runtime_dependency "pg", "~> 1.0"
  spec.add_runtime_dependency "dry-initializer", "> 2", "< 4"

  spec.add_development_dependency "bundler", ">= 1.16", "< 3"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "dotenv", "~> 2.2"
  spec.add_development_dependency "timecop", "~> 0.9"
  spec.add_development_dependency "pry", "~> 0.11"
end
