const cluster = require("cluster");
const fs = require("fs");
const path = require("path");
const workers = [];
const { promisify } = require("util");
const readdir = promisify(fs.readdir);
const stat = promisify(fs.stat);
const stream = require("stream");

if (cluster.isMaster) {
  masterProcess();
} else {
  childProcess();
}

async function findFilePathsByExtension(base, ext) {
  const subDirectories = await readdir(base);
  const files = await Promise.all(
    subDirectories.map(async subDirectory => {
      const fileRegexp = new RegExp(`.${ext}$`, "g");
      const newBase = path.join(base, subDirectory);
      if ((await stat(newBase)).isDirectory()) {
        return await findFilePathsByExtension(newBase, ext);
      } else if (fileRegexp.test(subDirectory)) {
        return newBase;
      }

      return null;
    })
  );

  const filteredFiles = files.reduce((previous, current) => {
    if (current) {
      return previous.concat(current);
    }
    return previous;
  }, []);

  return filteredFiles;
}

async function masterProcess() {
  console.log(`Master ${process.pid} is running`);
  let writeStreams = {};
  const files = await findFilePathsByExtension(path.resolve("src/"), "json");

  const worker = cluster.fork();
  workers.push(worker);
  worker.on("online", function() {
    worker.send({
      type: "PARSE_FILES",
      data: files
    });
  });

  worker.on("message", function({ type, data, name }) {
    const writer = writeStreams[name];
    const fileName = name.split("/")[name.split("/").length - 1];
    switch (type) {
      case "START_READING_DATA":
        writeStreams[name] = fs.createWriteStream(
          path.resolve(`out/${fileName}`)
        );
        break;
      case "RECEIVING_DATA_CHUNCK":
        writeStreams[name].write(new Buffer(data));
        break;
      case "FINISH_READING_FILE":
        writer.close();
        delete writeStreams[name];
        break;
    }
  });
}

async function readAndTransformFile(url, pipe) {
  const chunkSize = 500;
  const transformer = new stream.Transform({
    objectMode: true,
    highWaterMark: chunkSize
  });

  transformer._transform = function(chunk, encoding, done) {
    let preparedData = chunk;
    // @todo replace with better solution
    const isLastChunk = Buffer.byteLength(chunk) < chunkSize;
    if (isLastChunk) {
      const data = chunk.toString().split("\n");
      if (data.length > 1) {
        data.splice(-1, 0, `,"timestamp": "${new Date().toISOString()}"\n`);
      }
      preparedData = new Buffer(data.join(""), encoding);
    }
    this.push(preparedData);
    done();
  };

  fs.createReadStream(url, {
    highWaterMark: chunkSize,
    objectMode: true
  })
    .pipe(transformer)
    .pipe(pipe);
}

async function childProcess() {
  console.log(`Worker ${process.pid} started`);
  process.on("message", async function({ type, data }) {
    if (type === "PARSE_FILES") {
      data.forEach(url => {
        const writableStream = new stream.Writable({
          objectMode: true,
          highWaterMark: 100
        });

        process.send({
          type: "START_READING_DATA",
          name: url
        });

        writableStream._write = function(chunk, enc, next) {
          process.send({
            type: "RECEIVING_DATA_CHUNCK",
            name: url,
            data: chunk
          });
          next();
        };

        writableStream.on("finish", function() {
          process.send({
            type: "FINISH_READING_FILE",
            name: url
          });
        });
        readAndTransformFile(url, writableStream);
      });
    }
  });
}
