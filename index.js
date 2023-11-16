const fs = require("fs");
const { parse } = require("csv-parse");

require("dotenv").config({ path: __dirname + "./.env" });

const {Client} = require("pg");

const client = new Client(process.env.DATABASE);

const processor = async () => {
  let parser;
  try {
    parser = fs.createReadStream(`${__dirname}/dataset.csv`).pipe(parse());
  } catch (err) {
    console.error("Error reading file:", err);
  }

  const records = await new Promise((resolve) => {
    const books = [];
    parser.on("data", (record) => {
      const book = {
        id: record[0],
        title: record[1],
        author: record[2],
        authorLast: record[3],
        additionalAuthors: record[4],
        isbn: cleanISBN(record[5]),
        isbn13: cleanISBN(record[6]),
        myRating: record[7],
        avgRating: record[8],
        publisher: record[9],
        binding: record[10],
        pages: record[11],
        published: record[12],
        dateAdded: record[15],
      };
      books.push(book);
    });
    parser.on("end", () => {
      resolve(books);
    });
  });

  return records;
};

function cleanISBN(isbn) {
  const clean = isbn.replace("=", "").replaceAll(`"`, "");
  return clean;
}

async function seedDB() {
  
}

// TODO: separate author and book tables

(async () => {
  const records = await processor();
  console.log(records);
})();
