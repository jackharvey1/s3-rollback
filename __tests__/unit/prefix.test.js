const { getPrefix } = require('../../src/rollback');

describe('Parsing out the prefix', function () {
    it('fails when uri is invalid', function () {
        const badGetPrefixCall = getPrefix.bind(null, 'bucket', '/bucket/blah');
        expect(badGetPrefixCall).to.throw(Error);
    });

    it('fails when bucket in uri does not match bucket provided', function () {
        const badGetPrefixCall = getPrefix.bind(null, 'bucket', 's3://another-bucket/blah');
        expect(badGetPrefixCall).to.throw(Error);
    });

    it('gets the prefix', function () {
        expect(getPrefix('bucket', 's3://bucket/path/to/some/file')).to.equal('path/to/some/file');
    });
});
