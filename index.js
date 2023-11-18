#!/usr/bin/env node

const { parseArgs } = require("node:util");
const fs = require("fs");
const { parse } = require("csv-parse");

require("dotenv").config();

const { Client } = require("pg");
const client = new Client({
  password: process.env.PASSWORD,
  user: process.env.USERNAME,
  database: "shelfish",
  port: 5432,
  host: "localhost",
});

client.on("error", (err) => {
  console.error(err.message);
});


async function main() {

  // TODO: flags for env props/default
  // destructure argv
  const {
    values: { file },
  } = parseArgs({
    options: {
      file: {
        type: "string",
        short: "f",
        default: "not",
      },
    },
  });

  if (file === "not") {
    console.error(
      "Incorrect number of arguments.\n" +
        "\nPlease use format: node index.js -f <csv filename>" +
        "\n\nExiting process..."
    );

    process.exit(1);
  }

  const start = Date.now();

  const csvData = await processCSV(file);
  const { booksList, authorsList, junctionsList } = separateData(csvData);

  await initDB(client);
  const tableExists = await checkDBTable();

  seedDB(booksList, tableType.BOOK, client);

  const end = Date.now();

  console.log(`\nCount of books read: ${booksList.length}`);
  console.log(`Count of authors named: ${authorsList.length}`);
  console.log(`Count of relations made: ${junctionsList.length}\n`);

  console.log(`\n${end - start}ms`);

  process.exit(0);
}

/**
 * Process a CSV file and return an array of books.
 *
 * @param {string} filename - The name of the CSV file.
 * @returns {Promise<Array>} - A promise containing an array of book objects.
 */
const processCSV = async (filename) => {
  console.log("\nSpooling file parser..");

  let parser;
  try {
    parser = fs.createReadStream(`${__dirname}/${filename}`).pipe(parse());
    console.log("Parser charged and ready..");
  } catch (err) {
    console.error("Error reading file:", err);
  }

  const records = await new Promise((resolve) => {
    const books = new Array();

    parser.on("data", (record) => {
      const book = {
        title: record[0],
        authors: iterAuthors(record[1]),
        avgRating: record[2],
        isbn: cleanISBN(record[3]),
        isbn13: cleanISBN(record[4]),
        language: record[5],
        pages: record[6],
        ratingCount: record[7],
        textReviewCount: record[8],
        published: record[9],
        publisher: record[10],
      };
      books.push(book);
    });
    parser.on("end", () => {
      resolve(books);
    });
  });

  console.log("Parser firing.");

  return records;
};

/**
 * Simply splits a string of authors into an array of individual author names.
 *
 * @param {string} authors - A string of authors.
 * @returns {Array} - An array of individual author names.
 */
function iterAuthors(authors) {
  return authors.split("/");
}

/**
 * Removes unneeded characters from an ISBN.
 *
 * @param {string} isbn - The ISBN to be cleaned.
 * @returns {string} The cleaned ISBN.
 */
function cleanISBN(isbn) {
  const clean = isbn.replace("=", "").replaceAll(`"`, "");
  return clean;
}

/**
 * Separates the given array of data into three different arrays: booksList, authorsList, and junctionsList.
 *
 * @param {Array} data - An array of book objects.
 * @returns {Object} - An object containing the booksList, authorsList, and junctionsList arrays.
 */
function separateData(data) {
  const booksList = new Array();
  const authorsList = [];
  const junctionsList = [];

  for (const book of data) {
    const current = formatedBook(book);

    booksList.push(current);

    const authorsArr = book.authors;
    const { authors, junctions } = divyAuthors(authorsArr, book.isbn13);

    authorsList.push(...authors);
    junctionsList.push(...junctions);
  }

  return { booksList, authorsList, junctionsList };
}

/**
 * Returns a new book with the appropriate fields.
 *
 * @param {object} book - The input book object with various fields.
 * @returns {object} - A new book object ready to insert into the database
 */
function formatedBook(book) {
  const date = book.published.split("/");
  const formatDate = new Date(date[2], date[1] - 1, date[0]);

  return {
    title: book.title,
    avgRating: Number.parseFloat(book.avgRating),
    isbn: book.isbn,
    isbn13: Number.parseInt(book.isbn13),
    language: book.language,
    pages: Number.parseInt(book.pages),
    ratingCount: Number.parseInt(book.ratingCount),
    textReviewCount: Number.parseInt(book.textReviewCount),
    published: formatDate.toLocaleDateString(),
    publisher: book.publisher,
  };
}

function formatedAuthor(author, newAuthorID) {
  return {
    author_id: newAuthorID,
    name: author,
  };
}

function formatedJunction(author, bookISBN13) {
  return {
    author_id: author,
    book_id: bookISBN13,
  };
}

function divyAuthors(data, bookISBN13) {
  const authors = new Array();
  const junctions = new Array();

  let authorId = 0;

  for (let j = 0; j < data.length; j++) {
    const newAuthorID = authorId++;

    const author = formatedAuthor(data[j], newAuthorID);
    authors.push(author);

    const junction = formatedJunction(data[j], bookISBN13);
    junctions.push(junction);
  }

  return { authors, junctions };
}

/**
 * Connects to the database.
 *
 * @returns {Promise} A promise that resolves into a database connection.
 */
const initDB = async (client) => {
  let db;
  try {
    db = await client.connect();
    console.log("Database connected.");
    return db;
  } catch (err) {
    console.error(`DB connect failure: ${err}`);
    return Promise.reject(err);
  }
};

/**
 * Checks if the necessary tables exist in the database and creates them if they don't.
 *
 * @param {object} db - The database connection object.
 * @returns {boolean} - Returns true if the tables are created successfully, otherwise returns false.
 */
async function checkDBTable() {
  const booksCreateString = `
    CREATE TABLE IF NOT EXISTS books (
      isbn13 BIGINT PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      avgRating DECIMAL(3, 2),
      isbn CHAR(10),
      language CHAR(5),
      pages INT NOT NULL,
      ratingCount INT,
      textReviewCount INT,
      published DATE NOT NULL,
      publisher VARCHAR(255)
    );
  `;

  const authorsCreateTable = `
    CREATE TABLE IF NOT EXISTS authors (
      author_id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL
    );
  `;

  const junctionTable = `
    CREATE TABLE IF NOT EXISTS books_authors (
      book_id BIGINT REFERENCES books(isbn13),
      author_id INT REFERENCES authors(author_id),
      PRIMARY KEY (book_id, author_id)
    );
  `;

  try {
    console.log("checking for table: 'books'");
    await client.query(booksCreateString);

    console.log("checking for table: 'authors'");
    await client.query(authorsCreateTable);

    console.log("checking for table: 'books_authors'");
    await client.query(junctionTable);
  } catch (err) {
    console.error(err);
    throw err;
  }

  return;
}

const tableType = Object.freeze({
  BOOK: "books",
  AUTHOR: "authors",
  JUNCTION: "book_authors",
});

/**
 *
 * @param {Array<Object>} data
 * @param {String} type
 */
async function seedDB(data, type, dbClient) {
  data = data.slice(1);

  for (let i = 0; i < data.length; i++) {
    if (type == "books") {
      if (typeof data[i].isbn13 !== "number") continue;
    }
    // TODO: proper formatting needed
    const dataFields = Object.keys(data[i]);
    const dataValues = Object.values(data[i]);

    const query =
      "INSERT INTO " +
      type +
      "(" +
      dataFields +
      ") VALUES(" +
      dataValues +
      ");";

    console.log(query + "\n");

    try {
      // const attempt = await dbClient.query(query);
      // console.log(attempt);
    } catch (err) {
      console.error(err);
    }
  }
}

main();
