const {Storage}=require('@google-cloud/storage');
const {BigQuery}=require('@google-cloud/bigquery');

//Loading into BigQuery for test on demo file
const bq=new BigQuery();
const projId='midterm-visaxen';
const datasetId='pdf_files';
const tableId='file-list';
let count_malicious = 0;
let count_benign = 0; 

exports.classifyFiles = async (file,context) => {
  const gcsFile = file;
  const fname = gcsFile.name;
  const storage = new Storage();
  const testBucket = storage.bucket(file.bucket);
  const maliBucket = storage.bucket('visaxen-pdftest-malicious');
  const benBucket = storage.bucket('visaxen-pdftest-benign');

  //Function to copy the path from test bucket to the destination bucket
  const copyFile = async (destBucket) => {
    const srcFile = testBucket.file(fname);
    const destFile = destBucket.file(fname);
    await srcFile.copy(destFile);
  }

  //Helper function to check if the file is present in the database and return its class
  const scanFiles = async () => {
    const query = `SELECT filename, Class FROM \`midterm-visaxen.pdf_files.file-list\` WHERE filename = '${fname}'`;
    const options = {
      query: query
    };

    const [rows] = await bq.query(options);
    rows.forEach(row => {
      if(row.Class == 'Malicious'){
        count_malicious++;
        copyFile(maliBucket);
      }
      else if(row.Class == 'Benign'){
        count_benign++;
        copyFile(benBucket);
      }
    });
  }

  await scanFiles();
  console.log(`Malicious Files: ${count_malicious}`);
  console.log(`Benign Files: ${count_benign}`);

  await testBucket.file(fname).delete();
}