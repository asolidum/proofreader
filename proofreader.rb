#!/usr/bin/env ruby
require 'zlib'
require 'hyperll'
require 'offline_geocoder'
require 'args_parser'
require 'csv'

def check_format(type, regex_string, header_value, allow_blank, col_value, col, lineno, args)
  if allow_blank && col_value == ""
    STDERR.puts "\e[33mWARN\e[0m: #{args[:filename]}:L:#{lineno}:C:#{col}:H:#{header_value} has no value" if args[:output_level] >= 2
  else
    regex = Regexp.new regex_string
    if !(col_value =~ regex)
      STDERR.puts "\e[31mERROR\e[0m: #{args[:filename]}:L:#{lineno}:C:#{col}:H:#{header_value} '#{col_value}' not valid #{type} type"
    end
  end
end

args = ArgsParser.parse ARGV do
  arg :filename, 'input file', :alias => :f
  arg :field_format, 'field format', :alias => :ff, :default => 'uuid,ad_id_type,app_id,app_id,uuid,user_id,text,text,os,version,am_type,ip_addr,ts_sec,num,ts_msec,uuid,num,num,lat,lon,cc,num,loc_context,loc_method,text,text'
  arg :delimiter, 'delimiter', :alias => :d, :default => '|'
#  arg :dau_cols, 'ad_id, lat, lon column numbers for tracking dau', :alias => :dc
  arg :blank_cols, 'allow blank values in these columns', :alias => :bc
#  arg :country_code, 'countries to track dau', :alias => :cc, :default => 'US,CA,GB,FR,DE,IT,ES'
  arg :output_lines, "processing output every 'x' lines", :alias => :ol
  arg :skip_lines, "skip first 'x' lines", :alias => :sl, :default => 0
  arg :output_level, 'output level', :alias => :ov, :default => 1
  arg :header, 'display header', :alias => :h
  arg :help, 'show help', :alias => '?'

  validate :filename, "file not found" do |filename|
    File.exist?(filename)
  end

  validate :blank_cols, "comma separated list (eg. 0,1,2)" do |c|
    cols = c.to_s
    cols =~ /^(\d+(,\d+)*)?$/
  end

  validate :dau_cols, "comma separated list (eg. 0,3,7)" do |c|
    cols = c.to_s
    cols =~ /^(\d+(,\d+)*)?$/
  end

end

if args.has_option? :help or !args.has_param?(:filename)
  STDERR.puts args.help
  exit 1
end

fields = []
fields = args[:field_format].split(',')
fields.each do |field|
  if !(['uuid', 'app_id', 'user_id', 'os',
        'version', 'ad_id_type', 'am_type', 'ip_addr', 'ts_sec',
        'ts_msec', 'lat', 'lon', 'cc', 'state', 'zip', 'loc_context', 'loc_method',
        'text', 'num', 'SKIP'].include? field)
    puts "\e[33mWARNING\e[0m: Ignoring invalid field format '#{field}'"
    fields.delete(field)
  end
end

infile = open(args[:filename])
gz = Zlib::GzipReader.new(infile)

line = gz.readline()
header_cols = line.split(args[:delimiter])
if args[:header]
  header_cols.each_with_index do |col, index|
    puts "#{index}: #{col}"
  end
  exit 1
end
if header_cols.size != fields.size
  STDERR.puts "\e[31mERROR\e[0m: Field format count (#{fields.size}) " \
              "and line item count (#{header_cols.size}) mismatch"
  exit(0)
end

dau_cols = []
cols = []
cols = args[:dau_cols].to_s.split(',') if args.has_param?(:dau_cols)
cols.each do |col|
  dau_cols[col.to_i] = true
end
blank_cols = []
cols = args[:blank_cols].to_s.split(',') if args.has_param?(:blank_cols)
cols.each do |col|
  blank_cols[col.to_i] = true
end

index = 0
stime = Time.now.utc
gz.each_line do |line|
  index += 1
  next if index <= args[:skip_lines]
  cols = CSV::parse_line(line, { :col_sep => ',', :quote_char => '"' })
#  cols = line.split(args[:delimiter])
  for i in 0..cols.size-1
    case fields[i]
    when 'uuid'
      check_format(fields[i], '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'app_id'
      check_format(fields[i], '[0-9a-fA-F]{64}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'user_id'
      check_format(fields[i], '[0-9a-fA-F]{32}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'os'
      check_format(fields[i], '(IOS|AND)', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'version'
      check_format(fields[i], '(\d+\.)?(\d+\.)?(\*|\d+)', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'ad_id_type'
      check_format(fields[i], '(IDFA|AAID)', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'am_type'
      check_format(fields[i], '[a-z]{2}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'ip_addr'
      check_format(fields[i], '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'ts_sec'
      check_format(fields[i], '[0-9]{10}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'ts_msec'
      check_format(fields[i], '[0-9]{13}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'lat'
      if !(cols[i].to_f >= -90 && cols[i].to_f <= 90)
        STDERR.puts "\e[31mERROR\e[0m: Line #{index}, col #{i} (#{cols[i]}) not valid lat"
      end
    when 'lon'
      if !(cols[i].to_f >= -180 && cols[i].to_f <= 180)
        STDERR.puts "\e[31mERROR\e[0m: Line #{index}, col #{i} (#{cols[i]}) not valid lon"
      end
    when 'cc' || 'state'
      check_format(fields[i], '[A-Z]{2}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'zip'
      check_format(fields[i], '[0-9]{5}', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'loc_context'
      check_format(fields[i], '(background|foreground)', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'loc_method'
      check_format(fields[i], '(BCN|GPS)', header_cols[i], blank_cols[i], cols[i], i, index, args)
    when 'text'
    when 'num'
    when 'SKIP'
    else
      STDERR.puts "\e[31mERROR\e[0m:  Line #{index}, col #{i} (#{cols[i]}) Unknown format '#{fields[i]}'"
    end

  end
  if (index % args[:output_lines]) == 0
    puts "Processing line #{index}"
  end
end
