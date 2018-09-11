from math import sqrt, floor, ceil, log

# This file contains reimplementations of functions
# from the d3 data visualization package and
# ensures consistency with web applications using d3

# Reimplementation of linear nice from d3-scale
'''
scale.nice = function(count) {
    if (count == null) count = 10;

    var d = domain(),
        i0 = 0,
        i1 = d.length - 1,
        start = d[i0],
        stop = d[i1],
        step;

    if (stop < start) {
      step = start, start = stop, stop = step;
      step = i0, i0 = i1, i1 = step;
    }

    step = tickIncrement(start, stop, count);

    if (step > 0) {
      start = Math.floor(start / step) * step;
      stop = Math.ceil(stop / step) * step;
      step = tickIncrement(start, stop, count);
    } else if (step < 0) {
      start = Math.ceil(start * step) / step;
      stop = Math.floor(stop * step) / step;
      step = tickIncrement(start, stop, count);
    }

    if (step > 0) {
      d[i0] = Math.floor(start / step) * step;
      d[i1] = Math.ceil(stop / step) * step;
      domain(d);
    } else if (step < 0) {
      d[i0] = Math.ceil(start * step) / step;
      d[i1] = Math.floor(stop * step) / step;
      domain(d);
    }

    return scale;
  };
'''
def linear_nice(domain, count=10):
	i0 = 0
	i1 = len(domain) - 1
	start = domain[i0]
	stop = domain[i1]
	step = None

	if stop < start:
		# this duplicates the behavior of the js code above
		# but the original intent is unclear
		step = start
		start = stop
		stop = step
		step = i0
		i0 = i1
		i1 = step

	step = tickIncrement(start, stop, count)

	if step > 0:
		start = floor(start/step) * step
		stop = ceil(stop/step) * step
		step = tickIncrement(start, stop, count)
	elif step < 0:
		start = ceil(start/step) * step
		stop = floor(stop/step) * step
		step = tickIncrement(start, stop, count)

	if step > 0:
		domain[i0] = floor(start/step) * step
		domain[i1] = ceil(stop/step) * step
		return domain
	elif step < 0:
		domain[i0] = ceil(start/step) * step
		domain[i1] = floor(stop/step) * step
		return domain


# Reimplementation of tickIncrement from d3-array
'''
var e10 = Math.sqrt(50),
    e5 = Math.sqrt(10),
    e2 = Math.sqrt(2);
export function tickIncrement(start, stop, count) {
  var step = (stop - start) / Math.max(0, count),
      power = Math.floor(Math.log(step) / Math.LN10),
      error = step / Math.pow(10, power);
  return power >= 0
      ? (error >= e10 ? 10 : error >= e5 ? 5 : error >= e2 ? 2 : 1) * Math.pow(10, power)
      : -Math.pow(10, -power) / (error >= e10 ? 10 : error >= e5 ? 5 : error >= e2 ? 2 : 1);
}
'''
e10 = sqrt(50)
e5 = sqrt(10)
e2 = sqrt(2)
def tickIncrement(start, stop, count):
	step = (stop - start) / max(0, count)
	power = floor(log(step)/log(10))
	error = step/10**power
	if power >= 0:
		return (10 if error >= e10 else 5 if error >= e5 else 2 if error >= e2 else 1) * 10**power
	else:
		return -10**-power / (10 if error >= e10 else 5 if error >= e5 else 2 if error >= e2 else 1)

