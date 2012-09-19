import time

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
