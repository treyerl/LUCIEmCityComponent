using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LuciCommunication
{
    public class MixedStream : Stream
    {
        private readonly byte[] _buffer;
        private readonly Encoding _encoding = Encoding.UTF8;
        private readonly Stream _ns;
        private readonly byte[] _newLine;
        private int _bPos, _bSize;

        public MixedStream(Stream ns, Encoding enc = null, int bufferSize = 1024*1024)
        {
            if (enc != null)
                _encoding = enc;
            _ns = ns;
            _buffer = new byte[bufferSize];
            _newLine = _encoding.GetBytes("\n");
        }

        public int BufferSize
        {
            get { return _buffer.Length; }
        }

        public override bool CanRead
        {
            get { return _ns.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _ns.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _ns.CanWrite; }
        }

        public override long Length
        {
            get { return _ns.Length; }
        }

        public override long Position
        {
            get { return _ns.Position; }
            set { _ns.Position = value; }
        }

        /// <summary>
        ///     Resets buffer - loses all unread data
        /// </summary>
        public void ResetBuffer()
        {
            _bPos = 0;
            _bSize = 0;
        }

        public override void Flush()
        {
            _ns.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _ns.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _ns.SetLength(value);
        }

        /// <summary>
        ///     write line into buffer
        /// </summary>
        /// <param name="s"></param>
        public void Write(string s)
        {
            var b = _encoding.GetBytes(s);
            _ns.Write(b, 0, b.Length);
        }

        public void WriteLine(string s)
        {
            var b = _encoding.GetBytes(s + '\n');
            _ns.Write(b, 0, b.Length);
        }

        public void Write(byte[] bytes)
        {
            _ns.Write(bytes, 0, bytes.Length);
        }

        /// <summary>
        ///     Reads the line from buffer, preserving remaining bytes
        /// </summary>
        /// <returns></returns>
        public string ReadLine()
        {
            var sb = new StringBuilder("");
            if (_bPos < _bSize)
            {
                string s;
                var idx = IndexOf(_buffer, _newLine, _bPos, _bSize);
                if (idx >= 0)
                {
                    s = _encoding.GetString(_buffer, _bPos, idx - _bPos);
                    _bPos = idx + _newLine.Length;
                    return s.TrimEnd('\r');
                }
                s = _encoding.GetString(_buffer, _bPos, _bSize - _bPos);
                _bPos += _encoding.GetByteCount(s);
                sb.Append(s);
                if (_bPos < _bSize)
                    Buffer.BlockCopy(_buffer, _bPos, _buffer, 0, _bSize - _bPos);
                _bSize -= _bPos;
                _bPos = 0;
            }
            // now buffer is almost empty
            int curRead;
            while ((curRead = _ns.Read(_buffer, _bSize, _buffer.Length - _bSize)) > 0)
            {
                _bSize += curRead;
                string s;
                var idx = IndexOf(_buffer, _newLine, _bPos, _bSize);
                if (idx >= 0)
                {
                    s = _encoding.GetString(_buffer, _bPos, idx - _bPos);
                    _bPos = idx + _newLine.Length;
                    return sb.Append(s.TrimEnd('\r')).ToString();
                }
                s = _encoding.GetString(_buffer, _bPos, _bSize - _bPos);
                _bPos += _encoding.GetByteCount(s);
                sb.Append(s);
                if (_bPos < _bSize)
                    Buffer.BlockCopy(_buffer, _bPos, _buffer, 0, _bSize - _bPos);
                _bSize -= _bPos;
                _bPos = 0;
            }
            return sb.ToString();
        }

        /// <summary>
        ///     Naively search for a pattern
        /// </summary>
        /// <param name="src"></param>
        /// <param name="pattern"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        private int IndexOf(byte[] src, byte[] pattern, int start, int end)
        {
            bool found;
            for (var i = start; i <= end - pattern.Length; i++)
            {
                found = true;
                for (var j = 0; j < pattern.Length; j++)
                    if (pattern[j] != src[i + j])
                    {
                        found = false;
                        break;
                    }
                if (found)
                    return i;
            }
            return -1;
        }


        public int Read(byte[] buffer)
        {
            return Read(buffer, 0, buffer.Length);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int haveRead = 0, curRead;
            // there are unread bytes in buffer, send first them
            if (_bPos < _bSize)
            {
                haveRead = Math.Min(_bSize - _bPos, count);
                Buffer.BlockCopy(_buffer, _bPos, buffer, offset, haveRead);
                _bPos += haveRead;
                if (haveRead == count)
                    return count;
            }
            // now _bPos == _bSize for sure (otherwise it would return to caller), so we can reset _buffer safely
            ResetBuffer();
            // now _buffer is empty
            while ((curRead = _ns.Read(_buffer, _bSize, _buffer.Length - _bSize)) > 0)
            {
                _bSize += curRead;
                // if we have read enough
                if (_bSize + haveRead >= count)
                {
                    var toRead = count - haveRead;
                    Buffer.BlockCopy(_buffer, 0, buffer, offset + haveRead, toRead);
                    _bPos += toRead;
                    return count;
                }
                // if _buffer is full
                if (_bSize == _buffer.Length)
                {
                    Buffer.BlockCopy(_buffer, 0, buffer, offset + haveRead, _bSize);
                    haveRead += _bSize;
                    ResetBuffer();
                }
            }
            // if we could not read anything
            if (_bSize == 0)
                return haveRead;
            // if we have read something, but not enough
            Buffer.BlockCopy(_buffer, 0, buffer, offset + haveRead, _bSize);
            haveRead += _bSize;
            ResetBuffer();
            return haveRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _ns.Write(buffer, offset, count);
        }
    }

}
