#!/usr/bin/ruby

require 'time'

current_ms = 0
delta_ms = 0.05
last_time = nil
leaders = []
data = File.open('data','w+')
$stdin.each do |line|
	if line =~ /^(\d+-\d+-\d+ \d+:\d+:\d+\.\d+)/
		ts = Time.parse($1)
		if last_time.nil? or ts - last_time > delta_ms
			data.write("#{current_ms * 1000}\t#{leaders.join("\t")}\n")
			current_ms += delta_ms
			last_time = ts
		end
	end
	if line =~ /P (\d+) R \d+: Leader=(-?\d+)/
		peer = $1.to_i
		leader = $2.to_i
		leaders[peer] = leader
	end
end
data.close

n = ARGV[0].to_i
gp = File.open('plot.gnuplot', 'w+')
gp.write("set terminal png size 2000,300\nset grid\nset output \"output.png\"\nset nokey\nset yrange [-2:#{n}]\n")
gp.write("plot ")
gp.write(1.upto(n).to_a.map{|x|"'data' using 1:#{x} with points"}.join(","))


