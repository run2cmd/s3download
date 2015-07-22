#!/usr/bin/ruby
# -*- coding: utf-8 -*-
=begin

Name:         s3download.rb
Description:  This script is to Automatically copy data from S3
              and put it to local server. Mostly designed for cron
Author:       run2cmd
Version:      0.2 - Added monitoring class by run2cmd
              0.1 - Initial by run2cmd

=end

require 'rubygems'
require 'aws-sdk'
require 'net/smtp'
require 'date'

# Set of variables
# Easier to maintain when in one place
pid_file = '/application/log/s3download.pid'
log_file = '/application/log/s3download.log'
access_key = 'my_key'
secret_key = 'my_secret'
aws_region = 'eu-west-1'
s3_bucket = 'my_bucket'
s3_path = 'file/to/copy'
local_path = '/application/files'
download_to = '/application/files'
mail_list = [ 'run2cmd@mail.com' ]
msg = 'Run successfull. No files found for download'
go = 0

class S3GetFiles

  attr_accessor :s3

  # Initialize connection to AWS S3
  def initialize(key,secret,region,bucket,path,local)
    @bucket = bucket
    @path = path
    @local = local
    AWS.config({
      :access_key_id => "#{key}",
      :secret_access_key => "#{secret}",
      :region => "#{region}",
    })
    @s3 = AWS::S3.new
  end

  # Get list of s3 files
  def get_s3()
    s3_list = []
    data_bucket = @s3.buckets["#{@bucket}"].objects.with_prefix("#{@path}")
    data_bucket.each do |data_file|
      unless data_file.key.eql? "#{@path}/"
        s3_list.push(data_file.key.split('/')[-1])
      end
    end
    return s3_list
  end

  # Get list of local files
  def get_local()
    local_list = []
    Dir.foreach("#{@local}") do |dir|
            local_list.push(dir)
    end
    return local_list
  end

  # Compare s3 and local files. It is cheking if each s3
  # is donwloaded localy if not then add to missing list
  def get_missing(lists3,listlocal)
    missing = []
    lists3.each do |item|
      unless listlocal.include? item or listlocal.include? @path
        missing.push(item)
      end
    end
    return missing
  end

  # Download files from s3.
  # I'm doing objects.with_prefix since found no direct pointer, but that should
  # not cause the issues since I'm calling entire path, and there is no way to have
  # 2 same files name in same s3 directory. Also I use stream to I/O download
  # and separate that to chunks so only chunk not entire file is being written to memory.
  # This method however needs file compare since (size) since lost chunks are not retransmitted.
  def get_files(list,dest)
    if list.length > 0
      list.each do |g2_file|
        @s3.buckets["#{@bucket}"].objects.with_prefix("#{@path}/#{g2_file}").each do |file|
          s3_size = file.content_length
          local_file_path = "#{dest}/#{g2_file.split('/')[-1]}"
          File.open("#{local_file_path}", 'wb') do |stream|
            file.read do |chunk|
              stream.write(chunk)
            end
          end
          local_size = File.size("#{local_file_path}")
          unless local_size = s3_size
            raise IOError, 'Downloaded file was corrupted. Will try again on next run'
          end
        end
      end
    end
  end
end

class Monitoring
  # General send mail deifinition.
  # There are not spaces in lines between MESSAGE_END since they are treated as text.
  # This will at some point be replaced with some mail library.
  def send_mail(message,status,send_to)
    server = `hostname`.strip
    user = `whoami`.strip
    message_header = <<MESSAGE_END
From: #{user}@#{server}
To: #{send_to.join(',')}
Subject: Fulfillment LiveRamp Europe data copy status #{status}

#{message}

MESSAGE_END
    Net::SMTP.start('localhost') do |smtp|
      smtp.send_message message_header, "#{user}@#{server}", send_to
    end
  end

  # Simple logging definition.
  # It can use both text and array variables.
  def log_things(file,things)
    date = Time.new.to_s
    if things.to_s.one?
      File.open("#{file}", 'a') do |f|
        f.puts("#{date}: #{things}")
      end
    else
      things_join = things.join(',')
      File.open("#{file}", 'a') do |f|
        f.puts("#{date}: #{things_join}")
      end
    end
  end

  def clean_up(log)
    if File.size(log) / 2**20 > 20
      File.rename(log, "#{log}.old")
    end
  end
end


# Check if job is already running, using pid file
unless File.exists?(pid_file)
  File.open(pid_file, 'w') { |f| f.write(Process.pid) }
else
  go = 1
end

# Initialize Monitoring class
mon = Monitoring.new

# Program main routines
# Simple method to continue if works fine and send mail on failure
# The logging was enabled to get better visibility
# for people who do not know the code
if go == 0
  begin
    ns3 = S3GetFiles.new(access_key,secret_key,aws_region,s3_bucket,s3_path,local_path)
  rescue => msg
    msgt = 'AWS Connection Initialization failed: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    mon.log_things(log_file,msgt)
    File.delete(pid_file)
    exit 1
  end

  begin
    s3_files = ns3.get_s3()
  rescue => msg
    msgt = 'S3 connection failed: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    mon.log_things(log_file,msgt)
    File.delete(pid_file)
    exit 1
  end

  begin
    local_files = ns3.get_local()
  rescue => msg
    msgt = 'Could not get local files: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    mon.log_things(log_file,msgt)
    File.delete(pid_file)
    exit 1
  end

  begin
    get_list = ns3.get_missing(s3_files,local_files)
  rescue => msg
        local_files = ns3.get_local()
  rescue => msg
    msgt = 'Could not get local files: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    mon.log_things(log_file,msgt)
    File.delete(pid_file)
    exit 1
  end

  begin
    get_list = ns3.get_missing(s3_files,local_files)
  rescue => msg
    msgt = 'Comparing lists failed: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    mon.log_things(log_file,msgt)
    File.delete(pid_file)
    exit 1
  end

  if get_list.length > 0
    begin
      ns3.get_files(get_list,download_to)
    rescue => msg
      msgt = 'Downloading from S3 failed: ' + msg
      mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
      mon.log_things(log_file,msgt)
      File.delete(pid_file)
      exit 1
    end
    msgOK = 'Files copied successfully'
    mon.send_mail(msgOK,'OK',mail_list)
  else
    get_list = 'Run successfull. There were no data files to download'
  end

  begin
    mon.log_things(log_file,get_list)
  rescue => msg
    msgt = 'Logging failed: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    File.delete(pid_file)
    exit 1
  end

  begin
    mon.clean_up(log_file)
  rescue => msg
    msgt = 'Clean up process failed: ' + msg
    mon.send_mail(msgt,'FAILED. Job rerun in 5 min',mail_list)
    File.delete(pid_file)
  end
  File.delete(pid_file)
