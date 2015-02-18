require 'ok_hbase/concerns/table/batch'

module OkHbase
  module Concerns
    module Table
      extend ActiveSupport::Concern

      SCANNER_DEFAULTS = {
          start_row: nil,
          stop_row: nil,
          row_prefix: nil,
          columns: nil,
          filter_string: nil,
          timestamp: nil,
          include_timestamp: false,
          caching: 1000,
          limit: nil
      }.freeze

      SCANNER2_DEFAULTS = {
          start_row: nil,
          stop_row: nil,
          columns: nil,
          caching: 100,
          max_versions: 1,
          time_range: nil
      }.freeze

      TGET_DEFAULTS = {
          row: nil,
          columns: nil,
          timestamp: nil,
          time_range: nil,
          max_versions: 1
      }.freeze

      TGETS_DEFAULTS = {
          rows: nil,
          columns: nil,
          timestamp: nil,
          time_range: nil,
          max_versions: 1
      }.freeze

      TCOLUMN_DEFAULTS = {
          family: nil,
          qualifier: nil,
          timestamp: nil
      }.freeze

      attr_accessor :table_name, :connection

      def table_name
        @table_name
      end

      def table_name=(val)
        @table_name = val
      end

      def self.connection
        @self.connection
      end

      def self.connection=(val)
        @self.connection = val
      end


      def families()
        descriptors = self.connection.client.getColumnDescriptors(self.connection.table_name(table_name))

        families = {}

        descriptors.each_pair do |name, descriptor|
          name = name[0...-1] # remove trailing ':'
          families[name] = OkHbase.thrift_type_to_dict(descriptor)
        end
        families
      end

      def regions
        regions = self.connection.client.getTableRegions(self.connection.table_name(table_name))
        regions.map { |r| OkHbase.thrift_type_to_dict(r) }
      end

      def row(row_key, columns = nil, timestamp = nil, include_timestamp = false)
        raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

        row_key.force_encoding(Encoding::UTF_8)

        rows = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          self.connection.client.getRowWithColumnsTs(self.connection.table_name(table_name), row_key, columns, timestamp, {})
        else
          self.connection.client.getRowWithColumns(self.connection.table_name(table_name), row_key, columns, {})
        end

        rows.empty? ? {} : _make_row(rows[0].columns, include_timestamp)
      end

      def rows(row_keys, columns = nil, timestamp = nil, include_timestamp = false)
        raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

        row_keys.map! { |r| r.force_encoding(Encoding::UTF_8) }

        return {} if row_keys.blank?

        rows = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          columns = _column_family_names() unless columns

          self.connection.client.getRowsWithColumnsTs(self.connection.table_name(table_name), row_keys, columns, timestamp, {})
        else
          self.connection.client.getRowsWithColumns(self.connection.table_name(table_name), row_keys, columns, {})
        end

        rows.map { |row| [row.row, _make_row(row.columns, include_timestamp)] }
      end

      def cells(row_key, column, versions = nil, timestamp = nil, include_timestamp = nil)

        row_key.force_encoding(Encoding::UTF_8)

        versions ||= (2 ** 31) -1

        raise TypeError.new "'versions' parameter must be a number or nil" unless versions.is_a? Integer
        raise ArgumentError.new "'versions' parameter must be >= 1" unless versions >= 1

        cells = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          self.connection.client.getVerTs(self.connection.table_name(table_name), row_key, column, timestamp, versions, {})
        else
          self.connection.client.getVer(self.connection.table_name(table_name), row_key, column, versions, {})
        end

        cells.map { |cell| include_timestamp ? [cell.value, cell.timestamp] : cell.value }
      end

      def scan(opts={})

        rows = [] unless block_given?
        opts = SCANNER_DEFAULTS.merge opts.select { |k| SCANNER_DEFAULTS.keys.include? k }


        raise ArgumentError.new "'caching' must be >= 1" unless opts[:caching] && opts[:caching] >= 1
        raise ArgumentError.new "'limit' must be >= 1" if opts[:limit] && opts[:limit] < 1

        if opts[:row_prefix]
          raise ArgumentError.new "'row_prefix' cannot be combined with 'start_row' or 'stop_row'" if opts[:start_row] || opts[:stop_row]

          opts[:start_row] = opts[:row_prefix]
          opts[:stop_row] = OkHbase::increment_string opts[:start_row]

        end
        opts[:start_row] ||= ''

        scanner = _scanner(opts)
        scanner_id = self.connection.client.scannerOpenWithScan(self.connection.table_name(table_name), scanner, {})

        fetched_count = returned_count = 0

        begin
          while true
            how_many = opts[:limit] ? [opts[:caching], opts[:limit] - returned_count].min : opts[:caching]

            items = if how_many == 1
              self.connection.client.scannerGet(scanner_id)
            else
              self.connection.client.scannerGetList(scanner_id, how_many)
            end

            fetched_count += items.length

            items.map.with_index do |item, index|
              if block_given?
                yield item.row, _make_row(item.columns, opts[:include_timestamp])
              else
                rows << [item.row, _make_row(item.columns, opts[:include_timestamp])]
              end
              return rows if opts[:limit] && index + 1 + returned_count == opts[:limit]
            end

            break if items.length < how_many
          end
        ensure
          self.connection.client.scannerClose(scanner_id)
        end
        rows
      end

      def scan2(opts={})

        opts_input = opts.clone

        raise TypeError.new "'columns' must be a list" if opts_input[:columns] && !opts_input[:columns].is_a?(Array)
        raise TypeError.new "'time_range' must be a list" if opts_input[:time_range] && !opts_input[:time_range].is_a?(Array)

        opts_input[:columns] = _tcolumn(opts_input[:columns]) rescue nil
        opts_input[:time_range] = _ttimerange(opts_input[:time_range]) rescue nil

        rows2 = [] unless block_given?
        opts_input = SCANNER2_DEFAULTS.merge opts_input.select { |k| SCANNER2_DEFAULTS.keys.include? k }

        raise ArgumentError.new "'caching' must be >= 1" unless opts_input[:caching] && opts_input[:caching] >= 1
        raise ArgumentError.new "'max_versions' must be >= 1" if opts_input[:max_versions] && opts_input[:max_versions] < 1

        scanner2 = _scanner2(opts_input)

        scanner_id = self.connection.client2.openScanner(self.connection.table_name(table_name), scanner2)

        fetched_count = returned_count = 0

        begin
          while true
            how_many = opts_input[:max_versions] ? [opts_input[:caching], opts_input[:max_versions] - returned_count].min : opts_input[:caching]

            items = self.connection.client2.getScannerRows(scanner_id, how_many)

            fetched_count += items.length
            items.map.with_index do |item, index|
              if block_given?
                yield item.row, _make_row2(item.columnValues)
              else
                rows2 << [item.row, _make_row2(item.columnValues)]
              end
              return rows2 if opts_input[:limit] && index + 1 + returned_count == opts_input[:limit]
            end

            break if items.length < how_many
          end
        ensure
          self.connection.client2.closeScanner(scanner_id)
        end
        rows2
      end

      def put(row_key, data, timestamp = nil)
        batch = self.batch(timestamp)

        batch.transaction do |batch|
          batch.put(row_key, data)
        end
      end

      def delete(row_key, columns = nil, timestamp = nil)
        if columns
          batch = self.batch(timestamp)
          batch.transaction do |batch|
            batch.delete(row_key, columns)
          end

        else
          timestamp ? self.connection.client.deleteAllRowTs(self.connection.table_name(table_name), row_key, timestamp, {}) : self.connection.client.deleteAllRow(self.connection.table_name(table_name), row_key, {})
        end
      end

      def get(opts={})

        opts_input = opts.clone

        raise TypeError.new "'columns' must be a list" if opts_input[:columns] && !opts_input[:columns].is_a?(Array)
        raise TypeError.new "'time_range' must be a list" if opts_input[:time_range] && !opts_input[:time_range].is_a?(Array)

        opts_input[:columns] = _tcolumn(opts_input[:columns]) rescue nil
        opts_input[:time_range] = _ttimerange(opts_input[:time_range]) rescue nil

        opts_input = TGET_DEFAULTS.merge opts_input.select { |k| TGET_DEFAULTS.keys.include? k }
        tget = _tget(opts_input)
                
        item = self.connection.client2.get(self.connection.table_name(table_name), tget)
        if block_given?
          yield item.row, _make_row2(item.columnValues)
        else
          single_row = [item.row, _make_row2(item.columnValues)]
        end
        return single_row
      end

      def gets(opts={})
        
        opts_input = opts.clone

        raise TypeError.new "'rows' must be a list" if opts_input[:rows] && !opts_input[:rows].is_a?(Array)
        raise TypeError.new "'columns' must be a list" if opts_input[:columns] && !opts_input[:columns].is_a?(Array)
        raise TypeError.new "'time_range' must be a list" if opts_input[:time_range] && !opts_input[:time_range].is_a?(Array)
        
        opts_input[:columns] = _tcolumn(opts_input[:columns]) rescue nil
        opts_input[:time_range] = _ttimerange(opts_input[:time_range]) rescue nil
        
        opts_mod = opts_input.clone
        opts_mod[:rows] = nil

        opts_mod = TGETS_DEFAULTS.merge opts_mod.select { |k| TGETS_DEFAULTS.keys.include? k }

        ltget = Array.new(opts_input[:rows].count) { opts_mod.clone }
        mappings = {"rows" => "row"}
        Hash[ltget.map {|k, v| [mappings[k], v] }]
        opts_input[:rows].map.with_index{ |row, index| ltget[index][:row] = row }

        opts_return = []
        ltget.each do |ltget_single|
          opts_return << _tget(ltget_single)
        end
        items = self.connection.client2.getMultiple(self.connection.table_name(table_name), opts_return)
        
        rows = []
        items.map.with_index do |item, index|
          if block_given?
            yield item.row, _make_row2(item.columnValues)
          else
            rows << [item.row, _make_row2(item.columnValues)]
          end
        end
        rows
      end

      def batch(timestamp = nil, batch_size = nil, transaction = false)
        Batch.new(self, timestamp, batch_size, transaction)
      end

      def counter_get(row_key, column)
        counter_inc(row_key, column, 0)
      end

      def counter_set(row_key, column, value = 0)
        self.batch.transaction do |batch|
          batch.put(row_key, { column => [value].pack('Q>') })
        end
      end

      def counter_inc(row_key, column, value = 1)
        self.connection.client.atomicIncrement(self.connection.table_name(table_name), row_key, column, value)
      end

      def counter_dec(row_key, column, value = 1)
        counter_inc(row_key, column, -value)
      end

      def increment(increment)
        self.connection.increment(_new_increment(increment))
      end

      def increment_rows(increments)
        increments.map! { |i| _new_increment(i) }
        self.connection.increment(_new_increment(increments))
      end

      alias_method :find, :scan

      def _column_family_names()
        self.connection.client.getColumnDescriptors(self.connection.table_name(table_name)).keys()
      end

      def _scanner(opts)
        scanner = Apache::Hadoop::Hbase::Thrift::TScan.new()
        scanner_fields = Apache::Hadoop::Hbase::Thrift::TScan::FIELDS

        opts.each_pair do |k, v|
          const = k.to_s.upcase.gsub('_', '')
          const_value = Apache::Hadoop::Hbase::Thrift::TScan.const_get(const) rescue nil
          if const_value
            v.force_encoding(Encoding::UTF_8) if v.is_a?(String)
          #  OkHbase.logger.info "setting scanner.#{scanner_fields[const_value][:name]}: #{v}"
            scanner.send("#{scanner_fields[const_value][:name]}=", v)
          else
          end
        end
        scanner

      end

      def _scanner2(opts)
        scanner2 = Apache::Hadoop::Hbase::Thrift2::TScan.new()
        scanner_fields = Apache::Hadoop::Hbase::Thrift2::TScan::FIELDS

        opts.each_pair do |k, v|
          const = k.to_s.upcase.gsub('_', '')
          const_value = Apache::Hadoop::Hbase::Thrift2::TScan.const_get(const) rescue nil

          if const_value
            v.force_encoding(Encoding::UTF_8) if v.is_a?(String)
            scanner2.send("#{scanner_fields[const_value][:name]}=", v)
          else
          end
        end
        scanner2
      end

      def _make_row(cell_map, include_timestamp)
        row = {}
        cell_map.each_pair do |cell_name, cell|
          row[cell_name] = include_timestamp ? [cell.value, cell.timestamp] : cell.value
        end
        row
      end

      def _make_row2(cell_maps)
        row = []
        cell_maps.each do |cell_map|
          row << {family: cell_map.family, qualifier: cell_map.qualifier, value: cell_map.value, timestamp: cell_map.timestamp}
        end
        row
      end

      def _ttimerange(time_range)
        time_range = {minStamp: time_range[0], maxStamp: time_range[1]}
        ttimerange = Apache::Hadoop::Hbase::Thrift2::TTimeRange.new(time_range)
        ttimerange
      end

      def _new_increment(args)
        args[:table] = self.table_name
        args
      end

      def _tget(opts)
        tget = Apache::Hadoop::Hbase::Thrift2::TGet.new()
        tget_fields = Apache::Hadoop::Hbase::Thrift2::TGet::FIELDS

        opts.each_pair do |k, v|
          const = k.to_s.upcase.gsub('_', '')
          const_value = Apache::Hadoop::Hbase::Thrift2::TGet.const_get(const) rescue nil

          if const_value
            v.force_encoding(Encoding::UTF_8) if v.is_a?(String)
            tget.send("#{tget_fields[const_value][:name]}=", v)
          else
          end
        end
        tget
      end

      def _tcolumn(opts)
        atcolumn = []
        opts.each do |opt|
          opt = TCOLUMN_DEFAULTS.merge opt.select { |k| TCOLUMN_DEFAULTS.keys.include? k }
          opt = Apache::Hadoop::Hbase::Thrift2::TColumn.new(opt)
          atcolumn << opt
        end
        atcolumn
      end
    end
  end
end
