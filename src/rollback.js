const { S3 } = require('aws-sdk');
const { readFile } = require('fs');
const { promisify } = require('util');
const { resolve } = require('path');
const parseArgs = require('minimist');
const bluebird = require('bluebird');

const s3 = new S3();

const errors = [];
let successfulDeletions = 0;
let successfulReads = 0;

const fileReader = promisify(readFile);

const concurrency = 256;

const timestamp = () => {
    const date = new Date();
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `[${hours}:${minutes}:${seconds}]`;
};

const log = (...text) => {
    console.log(`${timestamp()} ${text}`);
};

const printInPlace = (...text) => {
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(`${timestamp()} ${text}`);
};

const exitAndPrintErrorsIfAny = (earlyExit = false) => {
    errors.map(console.error);
    if (earlyExit && errors.length) {
        log(`${errors.length} errors occurred`);
        log('Exited safely. No write operations were performed');
        process.exit(1);
    }
};

const printHelp = () => {
    console.log(`Assume role prior to execution with "export AWS_PROFILE=<profile>"

node rollback.js --bucket <bucket> --file <file> --datetime YYYY-MM-DDTHH:MM:SS [--dryrun, --verify]

--bucket        Bucket to perform the rollback operation on
--file          Location of file containing references to objects should be formatted as an S3 URI,
                i.e. s3://bucket/path/to/object. Separate objects should be separated by a new line.
--datetime      Threshold after which files are reverted, not including those on the precise second; format is of form YYYY-MM-DDTHH:MM:SS
--dryrun        Do not perform any write operations, simply log operations which would occur
--verify        List the object versions which fit the parameters after rollback to double check all operations were successful`);
};

async function run () {
    const args = parseArgs(process.argv);

    if (args.help) {
        printHelp();
        process.exit(0);
    }

    const { bucket, file, datetime } = args;
    if (bucket && file && datetime) {
        await rollback(args);
    } else {
        console.log('Missing one or more required parameters');
        process.exit(1);
    }
}

async function rollback ({ bucket, file, datetime, dryrun, verify }) {
    const rollbackLocation = resolve(file);
    log(`Reading from ${rollbackLocation}`);

    const prefixes = await parseFilePrefixes(bucket, rollbackLocation);

    log(`${prefixes.length} objects specified in file.`);

    const rollbacks = await fetchAllRollbacks(bucket, prefixes, datetime);

    console.log('');
    log(`${rollbacks.length} file versions to be deleted for ${prefixes.length} objects`);

    if (dryrun) {
        rollbacks.forEach(({ Prefix, VersionId }) => {
            log(`(dryrun) Rolling back ${Prefix} to ${VersionId} in ${bucket}`);
        });
    } else {
        await bluebird.map(rollbacks, rollback => deleteFileVersion(bucket, rollback), { concurrency });
        console.log('');
        exitAndPrintErrorsIfAny();
    }

    if (verify && !dryrun) {
        successfulReads = 0;
        log('Verifying...');
        const remainingRollbacks = await fetchAllRollbacks(bucket, prefixes, datetime);
        console.log('');
        log(`${remainingRollbacks.length} reversions were not performed`);
    }
}

const deleteFileVersion = (Bucket, { Prefix: Key, VersionId }) =>
    s3.deleteObject({ Bucket, Key, VersionId }).promise()
        .then(() => {
            successfulDeletions++;
        })
        .catch(error => {
            errors.push({ Bucket, Key, VersionId, error });
        })
        .then(() => printInPlace(`${successfulDeletions} successes and ${errors.length} errors`));

const parseFilePrefixes = (bucket, rollbackLocation) =>
    fileReader(rollbackLocation, 'utf8')
        .then(URIs => URIs
            .split('\n')
            .filter(Boolean)
            .map(uri => getPrefix(bucket, uri))
        )
        .catch(({ message }) => {
            console.log(message);
            process.exit(1);
        });

function getPrefix (bucket, uri) {
    const regex = new RegExp('^s3://([^/]+)/(.+)$');
    if (!regex.test(uri)) {
        throw new Error(`Invalid URI "${uri}". Should be of form s3://bucket/path/to/file`);
    }

    const [, bucketInURI, prefix] = uri.match(regex);

    if (bucketInURI !== bucket) {
        throw new Error(`Bucket specified in URI "${uri}" does not match bucket passed in parameters`);
    }

    return prefix;
}

async function fetchAllRollbacks (bucket, prefixes, datetime) {
    const rollbacks = await bluebird
        .map(prefixes, prefix => getRollbackVersion(bucket, prefix, datetime), { concurrency });

    exitAndPrintErrorsIfAny();

    return [].concat(
        ...rollbacks.filter(prefixAndVersions => Array.isArray(prefixAndVersions) && prefixAndVersions.length)
    );
}

const getRollbackVersion = (Bucket, Prefix, cutoff) =>
    s3.listObjectVersions({ Bucket, Prefix }).promise()
        .then(objectInfo => {
            printInPlace(`${++successfulReads} objects' version information read`);
            return objectInfo
                .Versions
                .filter(version => new Date(version.LastModified) > new Date(cutoff))
                .sort((a, b) => new Date(a.LastModified) - new Date(b.LastModified))
                .map(({ VersionId }) => ({
                    Prefix,
                    VersionId
                }));
        })
        .catch(e => errors.push(e));

module.exports = {
    run,
    deleteFileVersion,
    getPrefix,
    getRollbackVersion
};
