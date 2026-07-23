const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {serie: Serie} = require('../app/CRUD')

test('get daily difference stats', async(t) => {

    await t.test("get daily difference stats", async (t) => {

        const stats = await Serie.getDailyDifferenceStats(80, new Date(1991,0,1), new Date(2021,0,1))
        assert(stats.n)
        assert(stats.n <= 30 * 365 + 30/4 )
        assert(stats.min_diff <= stats.q1)
        assert(stats.q1 <= stats.median)
        assert(stats.median <= stats.q3)
        assert(stats.q3 <= stats.p95)
        assert(stats.p95 <= stats.p99)
        assert(stats.p99 <= stats.p995)
        assert(stats.p995 <= stats.p998)
        assert(stats.p998 <= stats.p999)
        assert(stats.p999 <= stats.max_diff)
        assert(stats.stddev_diff > 0)
        assert(stats.stddev_diff < stats.max_diff - stats.min_diff)
        assert(stats.mean_diff < stats.max_diff)
        assert(stats.mean_diff > stats.min_diff)
    })
})
