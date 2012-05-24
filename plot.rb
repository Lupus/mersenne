#!/usr/bin/ruby

require 'time'

#  Add methods to Enumerable, which makes them available to Array
module Enumerable
 
  #  sum of an array of numbers
  def sum
    return self.inject(0){|acc,i|acc +i}
  end
 
  #  average of an array of numbers
  def average
    return self.sum/self.length.to_f
  end
 
  #  variance of an array of numbers
  def sample_variance
    avg=self.average
    sum=self.inject(0){|acc,i|acc +(i-avg)**2}
    return(1/self.length.to_f*sum)
  end
 
  #  standard deviation of an array of numbers
  def standard_deviation
    return Math.sqrt(self.sample_variance)
  end
 
end  #  module Enumerable

pnum = ARGV[0].to_i

ts = nil
first_ts = nil
election_time=0
leaders = []
measures = []
start_time = nil
#data = File.open('data','w+')
$stdin.each do |line|
	if line =~ /^(\d+-\d+-\d+ \d+:\d+:\d+\.\d+)/
		ts = Time.parse($1)
#		if last_time.nil? or ts - last_time > delta_ms
#			data.write("#{current_ms * 1000}\t#{leaders.join("\t")}\n")
#			current_ms += delta_ms
#			last_time = ts
#		end
	end
	first_ts = ts if first_ts.nil?
	if line =~ /P (\d+) R \d+: new round started/
		start_time = ts
		leaders = []
	end
	if line =~ /P (\d+) R \d+: Leader=(-?\d+)/
		peer = $1.to_i
		leader = $2.to_i
		leaders[peer] = leader
		leaders.map!{|l| l.nil? ? -1 : l}
		if start_time and leaders.size >= pnum and leaders.sort.uniq.size == 1 and leaders[0]  > -1
			measures << ts - start_time
			start_time = nil
		end
	end
end
total_time = ts - first_ts
#data.close

measures.map! {|m| m * 1000}
election_time = measures.sum / 1000.0
if measures.size > 0
print "%f\t" % ((total_time - election_time) / total_time.to_f)
	puts "%f\t%f" % [measures.average, measures.standard_deviation]
else
	print "0\t"
	puts "-1\t-1"
end



#n = ARGV[0].to_i
#gp = File.open('plot.gnuplot', 'w+')
#gp.write("set terminal png size 2000,300\nset grid\nset output \"output.png\"\nset nokey\nset yrange [-2:#{n}]\n")
#gp.write("plot ")
#gp.write(1.upto(n).to_a.map{|x|"'data' using 1:#{x} with points"}.join(","))


