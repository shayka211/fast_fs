//! Out-of-band bootstrap over a plain TCP socket (the `ucp_hello_world` pattern).
//!
//! TCP is used only to swap UCX worker addresses; the socket is closed right after.
//! UCX itself never touches TCP, so it never activates its TCP transport and the data
//! path stays on shared memory (or RDMA between hosts).
//!
//! Wire format: frames of `[u32 LE length][bytes]`.
//!   client → server: frame(client worker address)
//!   server → client: u32 LE token, frame(server worker address)

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use tracing::{info, warn};

const TIMEOUT: Duration = Duration::from_secs(10);
/// Worker addresses are a few hundred bytes; anything huge is garbage.
const MAX_FRAME: u32 = 64 << 10;

/// A client that completed the server-side handshake.
#[derive(Debug)]
pub struct Joined {
    pub token: u32,
    pub address: Vec<u8>,
    pub peer: SocketAddr,
}

/// Server side: accepts bootstrap connections on a background thread (so the data loop
/// never makes a syscall for it) and hands each joined client to the data loop.
pub fn spawn_acceptor(listener: TcpListener, my_address: Vec<u8>) -> mpsc::Receiver<Joined> {
    let (tx, rx) = mpsc::channel();
    let span = tracing::Span::current();
    thread::Builder::new()
        .name("bootstrap".into())
        .spawn(move || {
            let _span = span.entered();
            for (token, stream) in (0u32..).zip(listener.incoming()) {
                // A misbehaving client is logged and skipped; it never stops the acceptor.
                if let Err(e) = admit(stream, token, &my_address, &tx) {
                    warn!(token, error = %e, "bootstrap failed");
                }
            }
        })
        .expect("failed to spawn bootstrap thread");
    rx
}

fn admit(
    stream: io::Result<TcpStream>,
    token: u32,
    my_address: &[u8],
    tx: &mpsc::Sender<Joined>,
) -> io::Result<()> {
    let mut stream = stream?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    let peer = stream.peer_addr()?;
    let address = read_frame(&mut stream)?;
    // Hand the client to the data loop *before* acking, so by the time the client can
    // send its first message the server already knows its token.
    tx.send(Joined {
        token,
        address,
        peer,
    })
    .map_err(|_| io::Error::other("data loop has exited"))?;
    stream.write_all(&token.to_le_bytes())?;
    write_frame(&mut stream, my_address)?;
    info!(%peer, token, "peer bootstrapped; closing TCP");
    Ok(())
}

/// Client side: swaps worker addresses with the server and closes the socket.
/// Returns `(token, server worker address)`.
pub fn join(server: SocketAddr, my_address: &[u8]) -> io::Result<(u32, Vec<u8>)> {
    let mut stream = TcpStream::connect_timeout(&server, TIMEOUT)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    write_frame(&mut stream, my_address)?;
    let mut token = [0; 4];
    stream.read_exact(&mut token)?;
    let address = read_frame(&mut stream)?;
    Ok((u32::from_le_bytes(token), address))
}

fn write_frame(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| io::Error::other("frame too large"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(bytes)
}

fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len);
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds {MAX_FRAME}"),
        ));
    }
    let mut buf = vec![0; len as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_and_limit() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"worker-address").unwrap();
        assert_eq!(read_frame(&mut &buf[..]).unwrap(), b"worker-address");

        let huge = (MAX_FRAME + 1).to_le_bytes();
        assert!(read_frame(&mut &huge[..]).is_err());
    }

    #[test]
    fn join_and_accept_swap_addresses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let joined = spawn_acceptor(listener, b"server".to_vec());
        for expected_token in 0..2 {
            let (token, server_addr) = join(addr, b"client").unwrap();
            assert_eq!((token, &server_addr[..]), (expected_token, &b"server"[..]));
            let j = joined.recv().unwrap();
            assert_eq!((j.token, &j.address[..]), (expected_token, &b"client"[..]));
        }
    }
}
