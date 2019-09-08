const fs = require('fs');


function randomChar() {
    return String.fromCharCode(Math.floor(Math.random() * (122 - 65) + 65));
}

function randomString(length) {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    for (var i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

function randomRow(withComma) {
    return `"${randomString(20)}": "${randomString(20)}"${withComma ? "," : ""}\n`
}

async function writeHead(stream) {
    return new Promise((resolve, reject) => {
        stream.write("{\n", 'utf8', () => resolve());
    });
}
async function writeRow(stream) {
    return new Promise((resolve, reject) => {
        stream.write(randomRow(true), 'utf8', () => resolve());
    });
}

async function writeFinish(stream) {
    return new Promise((resolve, reject) => {
        stream.write(`${randomRow()}\n}`, 'utf8', () => resolve());
    });
}
async function createJSONs(sizes) {
    sizes.forEach(async size => {
        const stream = fs.createWriteStream(`src/test/dummy${size}.json`);
        await writeHead(stream);
        for (let i = 0; i < size; i++) {
            await writeRow(stream);
        }
        await writeFinish(stream);

        stream.end();
    });
}

createJSONs([1000, 10000, 100000, 1000000, 10000000]);