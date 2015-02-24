
var inherits = require('util').inherits;
var stream   = require('stream');
var nextTick = process.nextTick;

function StreamMerger(opts) {
    opts = opts || {
        errorHandling: 'stream'
    };
    stream.Duplex.call(this);
    this.rs = null;   // current ReadStream
    this.ws = null;   // current WriteStream
    this.writeQueued = null;         // Buffer to write 
    this.wsSize      = undefined;    // Maxinum bytes to write to WriteStream
    this.wsWritten   = 0;            // Bytes written to WriteStream
    this.basicErrorHandling = true;  // bubble 'error' event from underlying stream
    this.on('finish', function(){
        console.log('StreamMerger: write End');
        this.emit('OSEnd');
    });
    // check options
    if (opts.errorHandling == 'stream') {
        // same as node's stream error handling,
        // bubble error event from rs,ws 
        this.basicErrorHandling = true;
    }
    if (opts.errorHandling == 'extended') {
        // emit ISError, OSError for finer-grain control
        this.basicErrorHandling = false;
    }
}

inherits(StreamMerger, stream.Duplex);

StreamMerger.prototype._read = function() {
    if (this.rs) {this.rs.resume();}
}

// Readable: Readable/Duplex or null
StreamMerger.prototype.setRead = function(Readable) {
    if (null===Readable) {
        this.rs = null;
        return this;
    }
    if (Readable instanceof stream.Readable || Readable instanceof stream.Duplex) {
        this.rs = Readable;
        this.rs.on('data', function(chunk){
            console.log('StreamMerger: push '+chunk.length);
            if (!this.push(chunk)) {
                console.log('StreamMerger: read Buffer full');
                this.rs.pause();
            }
        }.bind(this));
        this.rs.on('end', function(){
            console.log('StreamMerger: InputStream End');
            this.rs = null; 
            this.emit('ISEnd');
        }.bind(this))
        this.rs.on('error', function(e){
            console.log('StreamMerger: InputStream Error');
            this.rs = null;
            if (this.basicErrorHandling) {
                this.emit('error', e);
            }else{
                this.emit('ISError', e);
            } 
        }.bind(this));
        return this;
    }
    throw new Error('Readable must be Readable or Duplex or null');
}

StreamMerger.prototype.endRead = function(){
    console.log('StreamMerger: read End');
    this.push(null);
}

StreamMerger.prototype._write  = function(chunk, encoding, callback) {
    this.writeQueued = {
        chunk: chunk,
        encoding: encoding,
        callback: callback
    };
    this._processWrite();
}

StreamMerger.prototype._getNextWriteSize  = function() {
    if (this.ws===null)          return 0;
    if (this.wsSize===undefined) return 9007199254740991;  // MAX_SAFE_INTEGER
                                 return this.wsSize - this.wsWritten;
}

StreamMerger.prototype._processWrite = function() {
    nextTick(StreamMerger.prototype.__processWrite.bind(this));
}

StreamMerger.prototype.__processWrite = function() {
    var nextWrite = this._getNextWriteSize();
    var q = this.writeQueued;
    var written;
    if (nextWrite>0 && q) {
        if (nextWrite >= q.chunk.length) { // write entire buffer
            this.ws.write(q.chunk, q.encoding, q.callback);
            written = q.chunk.length;
            this.writeQueued = null;
        }else{
            var cWrite = q.chunk.slice(0, nextWrite);
            var cBuff  = q.chunk.slice(nextWrite);
            this.ws.write(cWrite, q.encoding);
            q.chunk = cBuff;
            written = cWrite.length;
        }
        this.wsWritten += written;
        console.log('StreamMerger: pass '+written+' to underlying Stream');
        this.emit('OSWrite',written, this.wsWritten);
    }
    if (this.wsWritten == this.wsSize) {
        console.log('StreamMerger: writeStream full');
        this.emit('OSFull');   // require switch OutputStream
    }
}

StreamMerger.prototype.setWrite = function(Writable, size) {
    if (null===Writable) {
        this.ws=null;
        this.wsSize = undefined;
        return this;
    }
    if (Writable instanceof stream.Writable || Writable instanceof stream.Duplex) {
        this.ws=Writable;
        this.wsSize = size;
        this.wsWritten = 0;
        this._processWrite();
        this.ws.on('error', function(e){
            this.rs = null;
            if (this.basicErrorHandling) {
                this.emit('error', e);
            }else{
                this.emit('OSError', e);
            } 
        }.bind(this));
        return this;
    }
    throw new Error('Writable must be Writable or Duplex');
}

StreamMerger.prototype.endWrite  = StreamMerger.end;

