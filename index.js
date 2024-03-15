const {Storage}=require('@google-cloud/storage');
const {BigQuery}=require('@google-cloud/bigquery');

//Loading into BigQuery for test on demo file
const bq=new BigQuery();
const datasetId='pdf_files';
const tableId='file-list';


exports.classifyFiles = async (file,context) => {
  const storage = new Storage();
  const testBucket = storage.bucket('visaxen-pdftest');
  const maliBucket = storage.bucket('visaxen-pdftest-malicious');
  const benBucket = storage.bucket('visaxen-pdftest-benign');
  let count_malicious = 0;
  let count_benign = 0; 

  sortFiles();
  testRun();

  //Helper function to read filenames from BigQuery and sort them into respective class bucket
  const sortFiles = async () => {
    // The SQL query to run
    const query = `SELECT * FROM ${datasetId}.${tableId} LIMIT 2500`;
    const options = {
      query: query
    };
    // Run the query
    const [rows] = await bq.query(options);

    rows.forEach(row => {
      if(row.Class == 'Malicious'){
        count_malicious = count_malicious+1;
        copyFile(row.filename, maliBucket);
      }
      else if(row.Class == 'Benign'){
        count_benign = count_benign+1;
        copyFile(row.filename, benBucket);
      }
    })

    console.log(`Malicious Files: ${count_malicious}`);
    console.log(`Benign Files: ${count_benign}`);
  }

  // Function to copy file from the test bucket to a destination bucket
  async function copyFile(filename, destBucket) {
    await storage.bucket(testBucket).file(filename).copy(storage.bucket(destBucket).file(filename));
  }

  //Helper function to check if the file is present in the database and return its class
  const scanFiles = async (filename) => {
    const query = `SELECT filename, Class FROM ${datasetId}.${tableId} WHERE filename = ${filename}`;
    const options = {
      query: query
    };

    const [rows] = await bigquery.query(options);
    rows.forEach(row => {
      if(row.count > 0)
        console.log(`${filename}: ${row.Class}`);
      else
        console.log("File Not Present");
    })
  }

  //Helper function to go through the files in the test bucket
  async function testRun () {
    const [files] = await testBucket.getFiles();
    files.forEach(file => {
      scanFiles(file.name);
    });
  }
}