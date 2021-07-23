const QuickBooks = require('node-quickbooks');
const { Coda } = require('coda-js');
const Sentry = require("@sentry/node");
const Tracing = require("@sentry/tracing");

require('dotenv').config();

Sentry.init({
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
});

const Constants = {
  CODA_API_KEY: process.env.CODA_API_KEY,
  CONSUMER_KEY: process.env.CONSUMER_KEY,
  CONSUMER_SECRET: process.env.CONSUMER_SECRET,
  REALM_ID: process.env.REALM_ID,
  OAUTH_ACCESS_TOKEN: process.env.OAUTH_ACCESS_TOKEN,
  OAUTH_REFRESH_TOKEN: process.env.OAUTH_REFRESH_TOKEN,
  MONTH_NAMES:  [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  ]
};

// Instances
const coda = new Coda(Constants.CODA_API_KEY);
const qbo = new QuickBooks(Constants.CONSUMER_KEY,
                         Constants.CONSUMER_SECRET,
                         Constants.OAUTH_ACCESS_TOKEN,
                         false,
                         Constants.REALM_ID,
                         false,
                         false,
                         null,
                         '2.0',
                         Constants.OAUTH_REFRESH_TOKEN);

// Months are 1 indexed
const getDaysInMonth = function(month = 1, year) {
  return new Date(year, month, 0).getDate();
};

// Months are 0 indexed
const loadQBODataForMonth = async (month = 0, year = 2020) => {
  return new Promise((resolve, reject) => {
    const paddedMonth = `${month + 1}`.padStart(2, '0');
    qbo.reportProfitAndLoss({
      start_date: `${year}-${paddedMonth}-01`,
      end_date: `${year}-${paddedMonth}-${getDaysInMonth(month + 1, year)}`
    }, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

const loadQBODataForYear = async (year = 2020) => {
  return new Promise((resolve, reject) => {
    qbo.reportProfitAndLoss({
      start_date: `${year}-01-01`,
      end_date: `${year}-12-31`
    }, function(err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

// Refresh QBO Token
const refreshQBOAccessToken = async () => {
  return new Promise((resolve, reject) => {
    qbo.refreshAccessToken(function (err, data) {
      if (err) reject(err);
      else resolve(data);
    });
  });
}

const MonthlyMappings = {
  "Gross Revenue": "Total Income",
  "Gross Expenses": "Total Expenses",

  "Gross Payroll": "Total [SC] Payroll",
  "Gross Benefits": "Total [SC] Benefits, Contributions & Tax",
  "Gross Subcontractors": "Total [SC] Subcontractors",

  "Sanctu Revenue": "[SC] Development Services",
  "Sanctu Payroll": "[SC] Development Payroll",
  "Sanctu Benefits": "[SC] Development Benefits, Contributions & Tax",
  "Sanctu Subcontractors": "[SC] Development Subcontractors",

  "Hydro Revenue": "[SC] Product Design Services",
  "Hydro Payroll": "[SC] Product Design Payroll",
  "Hydro Benefits": "[SC] Product Design Benefits, Contributions & Tax",
  "Hydro Subcontractors": "[SC] Product Design Subcontractors",

  "XXIX Revenue": "[SC] Brand Design Services",
  "XXIX Payroll": "[SC] Brand Design Payroll",
  "XXIX Benefits": "[SC] Brand Design Benefits, Contributions & Tax",
  "XXIX Subcontractors": "[SC] Brand Design Subcontractors",

  "Index Revenue": "[SC] Community Sales",
  "Index Supplies & Materials": "[SC] Community Supplies & Materials",
  "Index Payroll": "[SC] Community Payroll",
  "Index Benefits": "[SC] Community Benefits, Contributions & Tax",
  "Index Subcontractors": "[SC] Community Subcontractors",
};

const YearlyMappings = {
  "Gross Revenue": "Total Income",
  "Gross Expenses": "Total Expenses",
  "Gross COGS": "Total Cost of Goods Sold",
  "Profit Share Pool": "[SC] Profit Share, Bonuses & Misc",
  "Carbon Offsets": "[SC] Carbon Offsets",
  "Charitable Donations": "[SC] Charitable Donations",
  "Reinvestment": "[SC] Reinvestment"
};

const upsertDataForMonth = async (table, rows, month = 0, year = 2020, qboData) => {
  const rowName = `${Constants.MONTH_NAMES[month]}, ${year}`;
  const rowData = Object.keys(MonthlyMappings).map(key => {
    const qboCategory = MonthlyMappings[key];
    const value = parseFloat(qboData[qboCategory] || '0');
    return { column: key, value }
  });
  const rowDataWithMonth = [{ column: "Month", value: rowName }, ...rowData];

  const existingRow = rows.find(r => r.values.Month === rowName);
  if (existingRow) {
    await existingRow.update(rowDataWithMonth);
  } else {
    await table.insertRows([rowDataWithMonth]);
  }

  console.log(`Done for ${rowName}`);
};

const upsertDataForYear = async (table, rows, year = 2020, qboData) => {
  const rowName = year;
  const rowData = Object.keys(YearlyMappings).map(key => {
    const qboCategory = YearlyMappings[key];
    const value = parseFloat(qboData[qboCategory] || '0');
    return { column: key, value }
  });
  const rowDataWithYear = [{ column: "Year", value: rowName }, ...rowData];

  const existingRow = rows.find(r => r.values.Year === rowName);

  if (existingRow) {
    await existingRow.update(rowDataWithYear);
  } else {
    await table.insertRows([rowDataWithYear]);
  }

  console.log(`Done for ${rowName}`);
};

const loadAndUpsertDataForMonth = async (table, rows, month = 0, year = 2020) => {
  const data = await loadQBODataForMonth(month, year);

  /* Income */
  const rawIncomeData = data.Rows.Row.find(r => r.group === 'Income');
  const income = rawIncomeData ? rawIncomeData.Rows.Row.reduce((acc, r) => {
    acc[r.ColData[0].value] = r.ColData[1].value;
    return acc;
  }, {
    [rawIncomeData.Summary.ColData[0].value]: rawIncomeData.Summary.ColData[1].value
  }) : {};

  /* COGS */
  const rawCOGSData =
    data.Rows.Row.find(r => r.group === 'COGS');

  const rawPayrollData =
    rawCOGSData.Rows.Row.find(r => r.Summary && r.Summary.ColData[0].value === "Total [SC] Payroll");
  const payroll = rawPayrollData ? rawPayrollData.Rows.Row.reduce((acc, r) => {
    acc[r.ColData[0].value] = r.ColData[1].value;
    return acc;
  }, {
    [rawPayrollData.Summary.ColData[0].value]: rawPayrollData.Summary.ColData[1].value
  }) : {};

  const rawSuppliesData =
    rawCOGSData.Rows.Row.find(r => r.Summary && r.Summary.ColData[0].value === "Total [SC] Supplies & Materials");
  const supplies = rawSuppliesData ? rawSuppliesData.Rows.Row.reduce((acc, r) => {
    acc[r.ColData[0].value] = r.ColData[1].value;
    return acc;
  }, {
    [rawSuppliesData.Summary.ColData[0].value]: rawSuppliesData.Summary.ColData[1].value
  }) : {};

  const rawBenefitsData =
    rawCOGSData.Rows.Row.find(r => r.Summary.ColData[0].value === "Total [SC] Benefits, Contributions & Tax");
  const benefits = rawBenefitsData ? rawBenefitsData.Rows.Row.reduce((acc, r) => {
    acc[r.ColData[0].value] = r.ColData[1].value;
    return acc;
  }, {
    [rawBenefitsData.Summary.ColData[0].value]: rawBenefitsData.Summary.ColData[1].value
  }) : {};

  const rawSubcontractorsData =
    rawCOGSData.Rows.Row.find(r => r.Summary && r.Summary.ColData[0].value === "Total [SC] Subcontractors");
  const subcontractors = rawSubcontractorsData ? rawSubcontractorsData.Rows.Row.reduce((acc, r) => {
    acc[r.ColData[0].value] = r.ColData[1].value;
    return acc;
  }, {
    [rawSubcontractorsData.Summary.ColData[0].value]: rawSubcontractorsData.Summary.ColData[1].value
  }) : {};

  const rawExpenseData = data.Rows.Row.find(r => r.group === 'Expenses');
  const expenses = {
    [rawExpenseData.Summary.ColData[0].value]: rawExpenseData.Summary.ColData[1].value
  };

  const allData = {
    ...income,
    ...payroll,
    ...supplies,
    ...benefits,
    ...subcontractors,
    ...expenses
  };

  await upsertDataForMonth(table, rows, month, year, allData);
};

const loadAndUpsertAggregateDataForYear = async (table, rows, year = 2020) => {
  const data = await loadQBODataForYear(year);

  const rawIncomeData = data.Rows.Row.find(r => (r.group || '').toLowerCase() === 'income');
  const income = {
    [rawIncomeData.Summary.ColData[0].value]: rawIncomeData.Summary.ColData[1].value
  };

  const rawCOGSData = data.Rows.Row.find(r => (r.group || '').toLowerCase() === 'cogs');

  const profitShare = {};
  const reinvestment = {};
  const cogs = {};

  if (rawCOGSData) {
    const rawProfitShareData = rawCOGSData.Rows.Row.find(r => (r.ColData && r.ColData[0].value) === "[SC] Profit Share, Bonuses & Misc");
    if (rawProfitShareData) {
      profitShare[rawProfitShareData.ColData[0].value] = rawProfitShareData.ColData[1].value;
    };

    const rawReinvestmentData = rawCOGSData.Rows.Row.find(r => (r.ColData && r.ColData[0].value) === "[SC] Reinvestment");
    if (rawReinvestmentData) {
      reinvestment[rawReinvestmentData.ColData[0].value] = rawReinvestmentData.ColData[1].value;
    };

    cogs[rawCOGSData.Summary.ColData[0].value] = rawCOGSData.Summary.ColData[1].value;
  }

  const rawExpenseData = data.Rows.Row.find(r => (r.group || '').toLowerCase() === 'expenses');
  const expenses = {
    [rawExpenseData.Summary.ColData[0].value]: rawExpenseData.Summary.ColData[1].value
  };

  const rawCarbonOffsetData = rawExpenseData.Rows.Row.find(r => (r.ColData && r.ColData[0].value) === "[SC] Carbon Offsets");
  if (rawCarbonOffsetData) {
    expenses[rawCarbonOffsetData.ColData[0].value] = rawCarbonOffsetData.ColData[1].value;
  };

  const rawCharitableDonationsData = rawExpenseData.Rows.Row.find(r => (r.ColData && r.ColData[0].value) === "[SC] Charitable Donations");
  if (rawCharitableDonationsData) {
    expenses[rawCharitableDonationsData.ColData[0].value] = rawCharitableDonationsData.ColData[1].value;
  };

  const allData = {
    ...income,
    ...cogs,
    ...expenses,
    ...profitShare,
    ...reinvestment
  };

  await upsertDataForYear(table, rows, year, allData);
}

(async () => {
  const transaction = Sentry.startTransaction({
    op: "Sync",
    name: "Sync data from QBO to Coda",
  });

  try {
    // We run this once a day, so we need a new access token
    const token = await refreshQBOAccessToken();

    // Just here to remind me how to pull stuff
    //const doc = await coda.getDoc('jSPYRcqGSS');
    //console.log(await doc.listTables());

    // Pull Coda stuff
    const monthlyTable = await coda.getTable('jSPYRcqGSS', 'grid-p-qLcca9h3');
    const monthlyRows = await monthlyTable.listRows({ useColumnNames: true });

    // Get the last month that we do this for
    const today = new Date();
    let year = today.getFullYear();
    let prevMonth = today.getMonth() - 1; // This is zero indexed, Jan is 0
    if (prevMonth === -1) {
      prevMonth = 11;
      year = year - 1;
    }

    // Loop through months starting here (January, 2020, when we broke things out by studio)
    let target = new Date(2020, 0, 1);
    while (target < new Date(year, prevMonth + 1, 1)) {
      await loadAndUpsertDataForMonth(monthlyTable, monthlyRows, target.getMonth(), target.getFullYear());
      target.setMonth(target.getMonth() + 1);
    }

    // Next, populate yearly tables
    const yearlyTable = await coda.getTable('jSPYRcqGSS', 'grid-W9Os7GBzkH');
    const yearlyRows = await yearlyTable.listRows({ useColumnNames: true });

    let firstYear = 2016;
    while (firstYear <= (today.getFullYear())) {
      await loadAndUpsertAggregateDataForYear(yearlyTable, yearlyRows, firstYear);
      firstYear = firstYear + 1;
    }
    console.log("DONE!");
    Sentry.captureMessage("Sync Complete");
  } catch (e) {
    console.log(e);
    Sentry.captureException(e);
  } finally {
    transaction.finish();
  }
})();
