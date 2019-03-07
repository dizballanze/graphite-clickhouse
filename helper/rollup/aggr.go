package rollup

import (
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/montanaflynn/stats"
)

func AggrSum(points []point.Point) (r float64) {
	for _, p := range points {
		r += p.Value
	}
	return
}

func AggrMax(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	for _, p := range points {
		if p.Value > r {
			r = p.Value
		}
	}
	return
}

func AggrMin(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	for _, p := range points {
		if p.Value < r {
			r = p.Value
		}
	}
	return
}

func AggrAvg(points []point.Point) (r float64) {
	if len(points) == 0 {
		return
	}
	r = AggrSum(points) / float64(len(points))
	return
}

func AggrAny(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	return
}

func AggrAnyLast(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[len(points)-1].Value
	}
	return
}

func AggrPercentile(percent float64) (func ([]point.Point) (float64)) {
	return func (points []point.Point) (r float64) {
		d := make([]float64, len(points))
		if len(points) > 0 {
			for i, p := range points {
				d[i] = p.Value
			}
			r, _ = stats.Percentile(d, percent)
		}
		return
	}
}
