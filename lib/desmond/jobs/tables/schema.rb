require 'set'

module Desmond
  # TODO needs tableutils from polizei
  # TODO needs redshift->pg compatible sql generator to be useful here instead of polizei

  ##
  # exports the table structure of a set of tables
  # in the background.
  # does NOT support compound pk, fk or unqiue constraints!
  #
  class TableStructureExportJob < Desmond::BaseJobNoJobId
    ##
    # actual job
    #
    def execute(job_id, user_id, options={})
      time = Time.now.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
      s3_bucket = options[:s3_bucket] || GlobalConfig.polizei('aws_export_bucket')
      s3_key    = options[:s3_key] || "table_structure_export_#{user_id}_#{time}.sql"

      table = {}
      schema_name = options[:schema_name]
      table_name = options[:table_name]
      table = { schema_name: schema_name, table_name: table_name } unless schema_name.nil? && table_name.nil?

      # keeping track of tables so they can be exported in the right order
      # regarding their foreign key dependencies
      @exported_tables = Set.new
      @waiting_tables = {}

      begin
        s3_writer = Desmond::Streams::S3::S3Writer.new(s3_bucket, s3_key, options.fetch(:s3, {}))

        # get data for tables
        tables = []
        RSPool.with do |connection|
          tables = get_tables_data_with_dependencies(connection, table)
        end

        s3_writer.write("---------- #{tables.size} tables exported ----------\n") unless options[:nospacer]
        tables.each do |tbl|
          # build sql
          table_sql = build_sql(
            tbl[:schema_name],
            tbl[:table_name],
            tbl[:columns],
            tbl[:constraints],
            tbl[:dist_style],
            tbl[:sort_dist_keys]
          )

          # write to S3
          if @exported_tables.superset?(tbl[:dependencies])
            # write SQL to file if all dependencies have been written
           write_to_s3(s3_writer, tbl[:schema_name], tbl[:table_name], table_sql, options)
          else
            # not all dependencies have been written, wait until they have been
            @waiting_tables[tbl[:full_table_name]] = {
              schema_name: tbl[:schema_name],
              table_name: tbl[:table_name],
              sql: table_sql,
              dependens_on: tbl[:dependencies]
            }
          end
        end

        # make sure all tables have been written
        unless @waiting_tables.empty?
          fail "Cyclic dependency? Could not clear all dependencies: #{@waiting_tables.keys}"
        end
        # everything went well
        { bucket: s3_bucket, key: s3_key }
      ensure
        s3_writer.close unless s3_writer.nil?
      end
    end

    private

    ##
    # returns all describing data about a table in a hash structure
    #
    def get_tables_data_with_dependencies(connection, table)
      # occasionally one of these queries can fail with the error:
      # relation with OID XXXX does not exist
      # see: http://www.postgresql.org/message-id/29508.1187413841@sss.pgh.pa.us
      columns        = TableUtils.get_columns(connection, table)
      constraints    = TableUtils.get_table_constraints(connection, table)
      dist_style     = TableUtils.get_dist_styles(connection, table)
      sort_dist_keys = TableUtils.get_sort_and_dist_keys(connection, table)

      table_names    = Set.new(columns.keys)
      dependencies   = calculate_tables_dependencies(constraints)

      tables = []
      columns.each do |full_table_name, col_defs|
        table_dependencies = dependencies[full_table_name] || Set.new

        # make sure we have the data for all dependencies
        unless table_names.superset?(table_dependencies)
          table_dependencies.each do |table_dependency|
            schema_name, table_name = deconstruct_full_table_name(table_dependency)
            tables += get_tables_data_with_dependencies(connection,
              schema_name: schema_name, table_name: table_name)
          end
        end

        # merge data and save in array of all tables
        tables << {
          schema_name: col_defs[0]['schema_name'],
          table_name: col_defs[0]['table_name'],
          full_table_name: full_table_name,
          dependencies: table_dependencies,
          columns: columns[full_table_name],
          constraints: constraints[full_table_name],
          dist_style: dist_style[full_table_name],
          sort_dist_keys: sort_dist_keys[full_table_name]
        }
      end
      return tables
    end

    ##
    # returns the dependencies of tables given its constraints
    #
    def calculate_tables_dependencies(constraints)
      constraints.hmap do |full_table_name, tbl_constraints|
        foreign_keys = []
        unless tbl_constraints.nil? || tbl_constraints.empty?
          foreign_keys = tbl_constraints.select do |constraint|
            constraint['constraint_type'] == 'f'
          end
        end
        Set.new(foreign_keys.map do |fk|
          TableUtils.build_full_table_name(fk['ref_namespace'], fk['ref_tablename'])
        end)
      end
    end

    ##
    # returns schema_name and table_name out of a full table name
    #
    def deconstruct_full_table_name(full_table_name)
      fail 'Not a full table name!' if full_table_name.count('.') == 0
      fail 'Dots in schema or table name not supported!' if full_table_name.count('.') > 1
      parts = full_table_name.split('.')
      return parts[0], parts[1]
    end

    ##
    # rebuilds and returns the SQL to recreate the given table
    #
    def build_sql(schema_name, table_name, columns, constraints, diststyle, sort_dist_keys)
      constraints ||= []
      sort_dist_keys ||= {}
      diststyle = diststyle['dist_style']
      sortkeys = sort_dist_keys['sort_keys'] || []
      distkey = sort_dist_keys['dist_key'] || nil

      schema_name_sql = self.class.escape_rs_identifier(schema_name)
      table_name_sql  = self.class.escape_rs_identifier(table_name)
      distkey_sql     = self.class.escape_rs_identifier(distkey) unless distkey.nil?
      sortkeys_sql    = sortkeys.map { |sk| self.class.escape_rs_identifier(sk) }.join(', ') unless sortkeys.nil?
      structure_sql   = "CREATE TABLE #{schema_name_sql}.#{table_name_sql}(\n"
      structure_sql  += columns.map do |column|
        column_name_sql  = self.class.escape_rs_identifier(column['name'])
        column_type_sql  = column['type']
        column_type_sql += "(#{column['varchar_len']})" unless column['varchar_len'].nil?
        column_type_sql += "(#{column['numeric_precision']}, #{column['numeric_scale']})" if column['type'].downcase == 'decimal' || column['type'].downcase == 'numeric'
        column_encoding_sql = (column['encoding'].downcase == 'none') ? 'raw' : column['encoding']

        column_sql  = "\t#{column_name_sql} #{column_type_sql}"
        column_sql += ' NULL'                                if column['is_nullable'] == 'YES'
        column_sql += ' NOT NULL'                            if column['is_nullable'] != 'YES'
        column_sql += " IDENTITY(#{column['identity']})" unless column['identity'].nil?
        column_sql += " DEFAULT #{column['default']}"    unless column['default'].nil?
        column_sql += " ENCODE #{column_encoding_sql}"   unless self.options[:no_column_encoding]
        column_sql
      end.join(",\n")
      structure_sql  += ",\n" unless constraints.empty?
      structure_sql  += constraints.map do |constraint|
        column_name_sql  = self.class.escape_rs_identifier(constraint['contraint_columnname'])
        if constraint['constraint_type'] == 'u'
          "\tUNIQUE (#{column_name_sql})"
        elsif constraint['constraint_type'] == 'p'
          "\tPRIMARY KEY (#{column_name_sql})"
        elsif constraint['constraint_type'] == 'f'
          ref_schema_name_sql = self.class.escape_rs_identifier(constraint['ref_namespace'])
          ref_table_name_sql  = self.class.escape_rs_identifier(constraint['ref_tablename'])
          ref_column_name_sql  = self.class.escape_rs_identifier(constraint['ref_columnname'])
          "\tFOREIGN KEY (#{column_name_sql}) REFERENCES #{ref_schema_name_sql}.#{ref_table_name_sql} (#{ref_column_name_sql})"
        else
          fail "Unsupported constraint_type '#{constraint['constraint_type']}'"
        end
      end.join(",\n")
      structure_sql  += "\n)\n"
      structure_sql  += "DISTSTYLE #{diststyle}\n"
      structure_sql  += "DISTKEY (#{distkey_sql})\n" unless distkey.nil?
      structure_sql  += "SORTKEY (#{sortkeys_sql})\n" unless sortkeys.empty?
      structure_sql  += ';'
      structure_sql
    rescue => e
      Que.log level: :error, message: "table #{schema_name}.#{table_name} caused trouble"
      raise e
    end

    ##
    # write +table_sql+ out to S3 and check if new dependencies have been fulfilled
    #
    def write_to_s3(s3_writer, schema_name, table_name, table_sql, options={})
      # write table out
      full_table_name = TableUtils.build_full_table_name(schema_name, table_name)
      s3_writer.write("\n---------- #{full_table_name} ----------\n") unless options[:nospacer]
      s3_writer.write(table_sql)
      s3_writer.write("\n\n") unless options[:nospacer]
      @exported_tables << TableUtils.build_full_table_name(schema_name, table_name)

      # check if any new dependencies were fulfilled
      dependencies_fulfilled = @waiting_tables.select do |full_table_name, tbl|
        @exported_tables.superset?(tbl[:dependens_on])
      end
      @waiting_tables = @waiting_tables.delete_if do |full_table_name, tbl|
        dependencies_fulfilled.has_key?(full_table_name)
      end
      # write newly fulfilled tables out recursivly
      dependencies_fulfilled.each do |full_table_name, tbl|
        write_to_s3(s3_writer, tbl[:schema_name], tbl[:table_name], tbl[:sql])
      end
    end

    ##
    # escapes a RedShift identifier
    #
    def self.escape_rs_identifier(name)
      if name.size > 63
        # redshift allows this, pg doesn't
        # quote and escape all quotes within
        "\"#{name.gsub('"'){'\"'}}\""
      else
        PG::Connection.quote_ident(name)
      end
    end
  end
end