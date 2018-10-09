module DirtyPipeline
  module PG
    module_function
    def multi(pg_result)
      pg_result.first&.values
    end

    def single(pg_result)
      pg_result.first&.values&.first
    end
  end
end
