use std::{io, task::Waker, time::Duration};

use mio::{event, Events, Interest, Poll, Token};
use slab::Slab;

#[derive(Debug)]
struct Wakers {
    reader: Option<Waker>,
    writer: Option<Waker>,
}

#[derive(Debug)]
pub struct Poller {
    poller: Poll,
    events: Events,
    active: Slab<Wakers>,
}

impl Poller {
    pub(crate) fn with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            poller: Poll::new()?,
            events: Events::with_capacity(capacity),
            active: Slab::new(),
        })
    }

    pub(crate) fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.poller.poll(&mut self.events, timeout)?;
        for event in &self.events {
            let wakers = self
                .active
                .get_mut(event.token().0)
                .expect("io event is registered but not found in poller");

            if event.is_readable() {
                if let Some(waker) = wakers.reader.take() {
                    waker.wake();
                }
            }
            if event.is_writable() {
                if let Some(waker) = wakers.writer.take() {
                    waker.wake();
                }
            }
        }
        self.events.clear();
        Ok(())
    }

    pub(crate) fn waker(&mut self) -> io::Result<mio::Waker> {
        mio::Waker::new(
            self.poller.registry(),
            Token(self.active.insert(Wakers {
                reader: None,
                writer: None,
            })),
        )
    }

    pub(crate) fn register<S>(&mut self, source: &mut S, interests: Interest) -> io::Result<usize>
    where
        S: event::Source + ?Sized,
    {
        let entry = self.active.vacant_entry();
        let key = entry.key();
        self.poller
            .registry()
            .register(source, Token(key), interests)?;
        entry.insert(Wakers {
            reader: None,
            writer: None,
        });
        Ok(key)
    }

    pub(crate) fn add(&mut self, id: usize, waker: Waker, interest: Interest) {
        let wakers: &mut Wakers = self.active.get_mut(id).unwrap();
        if interest.is_readable() {
            wakers.reader = Some(waker);
        } else if interest.is_writable() {
            wakers.writer = Some(waker);
        }
    }

    pub(crate) fn deregister<S>(&mut self, id: usize, source: &mut S)
    where
        S: event::Source + ?Sized,
    {
        self.active.remove(id);
        self.poller
            .registry()
            .deregister(source)
            .unwrap_or_else(|e| tracing::warn!("deregister polling event failed, e: {}", e));
    }
}
