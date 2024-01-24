const fs = require('fs');
const path = require('path');

// Check if the correct number of command line arguments are provided
if (process.argv.length < 4) {
    console.error('Usage: node createSymlinks.js <sourcesFilePath> <isForTemplate>');
    process.exit(1);
}

const sourcesFilePath = process.argv[2];
const isForTemplate = process.argv[3] === "true";
const targetFolder = isForTemplate ? "templates" : "artifacts";
console.log("------------------");
console.log("sourcesFilePath: ", sourcesFilePath);
console.log("targetFolder: ", targetFolder);
cleanFolders(false);
createSymbolicLinks();



function createSymbolicLinks() {
    if (!fs.existsSync(targetFolder)) {
        fs.mkdirSync(targetFolder);
    }

    // Read the source file and split it into an array of paths
    const sourcePaths = fs.readFileSync(sourcesFilePath, 'utf-8').split('\n');

    for (let sourcePath of sourcePaths) {
        sourcePath = sourcePath.trim();
        if (sourcePath) {
            createSymbolicLinksRecursively(sourcePath);
        }
    };
}

function createSymbolicLinksRecursively(sourcePath) {
    const sourceAbsolutePath = path.isAbsolute(sourcePath) ? sourcePath : path.join(process.env.PROJECT_DIR, sourcePath); //path.resolve(sourcePath);
    let relPath = sourcePath;
    let targetFolderResolved = targetFolder;

    if (isForTemplate) {
        let pathsRegexArrayToReplaceOnTarget = [];
        if (sourcePath.endsWith(".template")) {
            pathsRegexArrayToReplaceOnTarget = ["Implementation/SharedObjects/Templates", "Implementation/LogicalUnits/\\\\w+/Templates/"];
            targetFolderResolved = path.join(targetFolder, "modules");
        } else {
            pathsRegexArrayToReplaceOnTarget = ["Implementation/LogicalUnits/"]
            targetFolderResolved = path.join(targetFolder, "logical_units");
        }

        pathsRegexArrayToReplaceOnTarget.forEach(s => {
            if (s) {
                let regex = new RegExp(s);
                relPath = relPath.replace(regex, "");
            }
        });
    }
    const targetDir = path.join(targetFolderResolved, path.dirname(relPath));
    const targetPath = path.join(targetFolderResolved, relPath); //, path.basename(sourcePath));    

    if (!fs.existsSync(sourceAbsolutePath)) {
        console.error("*** Source path does not exist:", sourceAbsolutePath);
        process.exit(1);
    }

    if (!fs.existsSync(targetDir)) {
        fs.mkdirSync(targetDir, { recursive: true });
    }

    const isDir = fs.statSync(sourceAbsolutePath).isDirectory();
    if (isDir) {
        const folderContents = fs.readdirSync(sourceAbsolutePath);
        folderContents.forEach((item) => {
            const itemPath = path.join(sourcePath, item);
            createSymbolicLinksRecursively(itemPath, targetFolder, isForTemplate);
        });
    } else if (!fs.existsSync(targetPath)) {
        fs.symlinkSync(sourceAbsolutePath, targetPath, 'file');
        console.log(`New symbolic link: ${targetPath}`);
    }
}

function cleanFolders(cleanOnlySymbolicLinks) {
    if (cleanOnlySymbolicLinks) {
        deleteSymbolicLinksRecursively(targetFolder);
    } else {
        if (fs.existsSync(targetFolder)) {
            console.log(`Deleting folder:`, targetFolder);
            fs.rmSync(targetFolder, { recursive: true });
        }
    }
}

function deleteSymbolicLinksRecursively(targetFolder) {
    if (!fs.existsSync(targetFolder)) {
        return;
    }

    let files = fs.readdirSync(targetFolder);
    files.forEach(function (file) {
        const p = path.join(targetFolder, file);
        if (fs.existsSync(p)) {
            const stat = fs.lstatSync(p);
            if (stat.isSymbolicLink()) {
                console.log(`Deleting symbolic link:`, p);
                fs.rmSync(p, { recursive: true });
            } else if (stat.isDirectory()) {
                deleteSymbolicLinksRecursively(p);
            }
        } else {
            // assuming it is a symbolic link to a deleted file:
            console.log(`Deleting symbolic link:`, p);
            fs.rmSync(p, { recursive: true, force: true });
        }
    });
}

