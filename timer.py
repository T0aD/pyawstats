import time

class timeit():
    def __init__(self):
        self.start = time.time()
        self.save()
    def save(self):
        self.current = time.time()
    def show(self, msg):
        diff1 = '%.02f' % ((time.time() - self.start) * 1000)
        diff2 = '%.02f' % ((time.time() - self.current) * 1000)
        format = 'TIMER: %9s - %9s ms > %s'
        print(format % (diff1, diff2, msg))
        self.save()

class timer:
    av = []
    def __init__(self, lines = 0):
        self.time, self.lines = (time.time(), lines)
    def average(self):
        if not len(self.av):
            return
        total = 0
        count = 0
        for av in self.av:
            total += av
            count += 1
        print('Average: %5d lines per sec' % (total / count, ))

    def stop(self, msg, lines, size = False):
        ct = time.time()
        dt = ct - self.time
        dl = lines - self.lines
#        print('%f - %f = %f' % (lines, self.lines, dl))
        self.lines = lines
        self.time = ct
        lps = dl / dt
        info = '%5d l/s' % (lps,)
        self.av.append(int(lps))
        if dt < 0.5:
            print('%s took %0.2f ms %s' % (msg, dt * 1000, info))
        else:
            print('%s took %0.2f s %s' % (msg, dt, info))
