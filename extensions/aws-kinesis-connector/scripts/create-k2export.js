const fs = require('fs');
const path = require('path');
const JSZip = require('jszip');

// Check if the correct number of command line arguments are provided
if (process.argv.length < 3) {
    console.error('Usage: node createK2Export.js <targetFileName>');
    process.exit(1);
}

const targetFileName = process.argv[2];
createZip();


async function createZip() {

    const addFilesFromDirectoryToZip = async (directoryPath, zip) => {
        const directoryContents = fs.readdirSync(directoryPath);
        directoryContents.forEach(async (item) => {
            const p = path.join(directoryPath, item);
            let relativePath = p.replace("artifacts/", "").replace("templates/logical_units", "Implementation/LogicalUnits").replace("templates/modules", "Implementation/SharedObjects/Templates");

            if (fs.statSync(p).isFile()) {
                zip.file(relativePath, fs.readFileSync(p));
            }

            if (fs.statSync(p).isDirectory()) {
                await addFilesFromDirectoryToZip(p, zip);
            }
        });
    };

    const zip = new JSZip();
    if (fs.existsSync("artifacts")) {
        await addFilesFromDirectoryToZip("artifacts", zip);
    }
    if (fs.existsSync("templates/logical_units")) {
        await addFilesFromDirectoryToZip("templates/logical_units", zip);
    }
    if (fs.existsSync("templates/modules")) {
        await addFilesFromDirectoryToZip("templates/modules", zip);
    }

    const buffer = await zip.generateAsync({ type: "nodebuffer", compression: 'DEFLATE', compressionOptions: { level: 4 } });
    fs.writeFileSync(targetFileName, buffer);
}
