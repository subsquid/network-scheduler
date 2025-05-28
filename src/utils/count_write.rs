use std::io::{Result, Write};

pub struct CountWrite<W: Write> {
    inner: W,
    count: usize,
}

impl<W: Write> CountWrite<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CountWrite<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let written = self.inner.write(buf)?;
        self.count += written;
        Ok(written)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write_all(buf)?;
        self.count += buf.len();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}
