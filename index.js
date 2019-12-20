const EventEmitter = require('events');
const { Transform, PassThrough } = require('stream');

const uuid = require('uuid/v1');
const csv = require('csv-write-stream');
const QueryStream = require('pg-query-stream');

/**
 *
 * stream order:
 *
 * PG/Query => Detector => CSV => StringToBuffer => Control => S3
 * 
 */

class PostgresToS3 extends EventEmitter {
  static _stringToBufferTransformer() {
    // returns a stream Transformer that takes strings in and pushes buffers out
    return new Transform({
      transform(chunk, encoding, callback) {
        if (encoding === 'buffer') {
          return callback(null, chunk);
        }
        const asBuffer = Buffer.from(chunk, encoding);
        return callback(null, asBuffer);
      }
    });
  }

  constructor({ pool, s3, filter, bucket }) {
    super();

    // pool is required to be an instance of a `node-pg` pool
    this.pool = pool;

    this.s3 = s3;
    this.bucket = bucket;
    this.uploader = null;

    this.filter = filter;
    this.filterValue = null;

    this.streams = {
      query: null,
      detector: this.makeDetector(),
      csv: csv(),
      stringToBuffer: PostgresToS3._stringToBufferTransformer(),
      control: null,
    };
  }

  start(query) {
    this.pool.connect((err, client, done) => {
      const qs = client.query(query);

      qs.on('end', done);

      qs.pipe(this.streams.detector)
        .pipe(this.streams.csv)
        .pipe(this.streams.stringToBuffer);

      this.streams.query = qs;
    });
  }

  prepareNext() {
    // if there is a current control stream, stop sending csv data to it,
    // end it (which should stop the upload), and create a new one
    this.streams.stringToBuffer.unpipe(this.streams.control);
    this.streams.control.end();

    // take the data stream, pipe it through to the 
    this.streams.control = new PassThrough();
    this.streams.stringToBuffer.pipe(this.streams.control);

    const key = `${ uuid() }.csv`;
    this.uploader = s3.upload({
      Key: key,
      Bucket: this.bucket,
      Body: this.streams.control
    });

    this.uploader.on('httpUploadProgress', progress => {
      this.emit('progress', progress);
    });
  }

  makeDetector() {
    // returns a duplexer that passes through incoming values, simply for detecting
    // a change in the field so that we can create a new uploader
    return new Transform({
      transform(chunk, encoding, callback) {
        // if our filter function returns a new value, start a new upload
        const nextFilterValue = this.filter(chunk, filterValue);
        if (nextFilterValue) {
          this.filterValue = nextFilterValue;
          this.prepareNext();
        }

        // pass through chunk
        callback(null, chunk);
      }
    });
  }
}

module.exports = pgTools;
