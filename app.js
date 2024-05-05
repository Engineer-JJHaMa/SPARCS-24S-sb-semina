const express = require("express");
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

const app = express();

async function problem1() {
  const result = await prisma.customer.findMany({
    where: {
      income: {
        gte: 50000,
        lte: 60000,
      },
    },
    orderBy: [
      {
        income: "desc",
      },
      {
        lastName: "asc",
      },
      {
        firstName: "asc",
      },
    ],
    take: 10,
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
  });

  return result;
}

async function problem2() {
  const result = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: {
          in: ["London", "Berlin"],
        },
      },
    },
    select: {
      sin: true,
      salary: true,
      Branch_Employee_branchNumberToBranch: {
        select: {
          branchName: true,
          Employee_Branch_managerSINToEmployee: true,
        },
      },
    },
  });

  const res = result.map((row) => ({
    sin: row.sin,
    branchName: row.Branch_Employee_branchNumberToBranch.branchName,
    salary: row.salary,
    "Salary Diff":
      row.Branch_Employee_branchNumberToBranch
        .Employee_Branch_managerSINToEmployee.salary - row.salary,
  }));
  res.sort((a, b) => b["Salary Diff"] - a["Salary Diff"]);

  return res.slice(0, 10);
}

async function problem3() {
  const butlerIncomes = await prisma.customer.findMany({
    where: {
      lastName: "Butler",
    },
    select: {
      income: true,
    },
  });
  const butlerIncomesMax = Math.max(...butlerIncomes.map((row) => row.income));
  const customers = await prisma.customer.findMany({
    where: {
      income: {
        gte: butlerIncomesMax * 2,
      },
    },
    orderBy: [{ lastName: "asc" }, { firstName: "asc" }],
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
    take: 10,
  });

  return customers;
}

async function problem4() {
  const customers = await prisma.customer.findMany({
    select: {
      customerID: true,
      Owns: {
        select: {
          Account: {
            select: {
              Branch: {
                select: {
                  branchName: true,
                },
              },
            },
          },
        },
      },
    },
  });

  const tmp = customers
    .filter(
      (row) =>
        row.Owns.some((elem) => elem.Account.Branch.branchName === "London") &&
        row.Owns.some((elem) => elem.Account.Branch.branchName === "Latveria")
    )
    .map((row) => row.customerID);

  const accounts = await prisma.customer.findMany({
    where: {
      income: {
        gt: 80000,
      },
      customerID: {
        in: tmp,
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          Account: {
            select: {
              accNumber: true,
              branchNumber: true,
            },
          },
        },
      },
    },
    orderBy: [{ customerID: "asc" }],
  });

  const res = [];
  for (let row of accounts) {
    row.Owns.sort((a, b) => a.Account.accNumber - b.accNumber);
    for (let own of row.Owns) {
      res.push({
        customerID: row.customerID,
        income: row.income,
        accNumber: own.Account.accNumber,
        branchNumber: own.Account.branchNumber,
      });
    }
  }

  return res.slice(0, 10);
}

async function problem5() {
  const accounts = await prisma.account.findMany({
    where: {
      OR: [{ type: "BUS" }, { type: "SAV" }],
    },
    select: {
      Owns: {
        select: {
          customerID: true,
        },
      },
      type: true,
      accNumber: true,
      balance: true,
    },
  });

  const res = [];
  for (let row of accounts) {
    for (let own of row.Owns) {
      res.push({
        customerID: own.customerID,
        type: row.type,
        accNumber: row.accNumber,
        balance: row.balance,
      });
    }
  }

  res.sort((a, b) => (a.type < b.type ? -1 : 1));
  res.sort((a, b) => a.customerID - b.customerID);

  return res.slice(0, 10);
}

async function problem6() {
  const accounts = await prisma.account.findMany({
    where: {
      Branch: {
        Employee_Branch_managerSINToEmployee: {
          firstName: {
            equals: "Phillip",
          },
          lastName: {
            equals: "Edwards",
          },
        },
      },
    },
    select: {
      Branch: {
        select: {
          branchName: true,
        },
      },
      accNumber: true,
      balance: true,
    },
    orderBy: {
      accNumber: "asc",
    },
  });

  return accounts
    .map((row) => ({
      branchName: row.Branch.branchName,
      accNumber: row.accNumber,
      balance: Number(row.balance),
    }))
    .filter((row) => row.balance > 100000)
    .slice(0, 10);
}

async function problem7() {
  const newYorkCustomers = await prisma.customer.findMany({
    where: {
      AND: [
        {
          Owns: {
            some: {
              Account: {
                Branch: {
                  branchName: {
                    equals: "New York",
                  },
                },
              },
            },
            none: {
              Account: {
                Branch: {
                  branchName: {
                    equals: "London",
                  },
                },
              },
            },
          },
        },
        {
          NOT: {
            Owns: {
              some: {
                Account: {
                  Owns: {
                    some: {
                      Customer: {
                        Owns: {
                          some: {
                            Account: {
                              Branch: {
                                branchName: {
                                  equals: "London",
                                },
                              },
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      ],
    },
    select: {
      customerID: true,
    },
    distinct: ["customerID"],
    orderBy: {
      customerID: "asc",
    },
  });

  return newYorkCustomers.slice(0, 10);
}

async function problem8() {
  const employees = await prisma.employee.findMany({
    where: {
      salary: {
        gt: 50000,
      },
    },
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchName: true,
        },
      },
    },
    orderBy: {
      firstName: "asc",
    },
  });
  const res = employees.map((row) => ({
    sin: row.sin,
    firstName: row.firstName,
    lastName: row.lastName,
    salary: row.salary,
    branchName:
      row.Branch_Branch_managerSINToEmployee.length === 0
        ? null
        : row.Branch_Branch_managerSINToEmployee[0].branchName,
  }));
  res.sort((a, b) =>
    (a.branchName === null ? "" : a.branchName) >
    (b.branchName === null ? "" : b.branchName)
      ? -1
      : 1
  );

  return res.slice(0, 10);
}

async function problem9() {
  return problem8();
}

async function problem10() {
  const helenRanchNumbers = await prisma.branch.findMany({
    where: {
      Account: {
        some: {
          Owns: {
            some: {
              Customer: {
                firstName: "Helen",
                lastName: "Morgan",
              },
            },
          },
        },
      },
    },
    select: {
      branchNumber: true,
    },
    distinct: ["branchNumber"],
  });
  const arr = helenRanchNumbers.map((row) => row.branchNumber);

  const tmp = await prisma.customer.findMany({
    select: {
      customerID: true,
      firstName: true,
      lastName: true,
      income: true,
      Owns: {
        select: {
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
    orderBy: {
      income: "desc",
    },
  });

  const res = tmp.filter((row) => {
    for (let branchNum of arr) {
      if (!row.Owns.some((e) => e.Account.branchNumber === branchNum)) {
        return false;
      }
    }
    return true;
  });
  console.log(res);

  return res
    .map((row) => ({
      customerID: row.customerID,
      firstName: row.firstName,
      lastName: row.lastName,
      income: row.income,
    }))
    .slice(0, 10);
}
async function problem11() {
  const lowestPaidEmployees = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: "Berlin",
      },
    },
    orderBy: [
      {
        salary: "asc",
      },
      {
        sin: "asc",
      },
    ],
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
    },
    take: 1,
  });
  return lowestPaidEmployees;
}
async function problem14() {
  const moscowBranchSalarySum = await prisma.employee.aggregate({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: "Moscow",
      },
    },
    _sum: {
      salary: true,
    },
  });

  return { "sum of employees salaries": moscowBranchSalarySum._sum.salary };
}
async function problem15() {
  const customersWithFourBranchTypes = await prisma.customer.findMany({
    select: {
      customerID: true,
      firstName: true,
      lastName: true,
      Owns: {
        select: {
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
    orderBy: [
      {
        lastName: "asc",
      },
      {
        firstName: "asc",
      },
    ],
  });

  const tmp = customersWithFourBranchTypes.filter((row) => {
    let count = 0;
    let prev = [];
    for (let branch of row.Owns) {
      if (!prev.some((e) => e === branch.Account.branchNumber)) {
        prev.push(branch.Account.branchNumber);
        count += 1;
      }
    }
    return count === 4;
  });

  return tmp.map((row) => ({
    customerID: row.customerID,
    firstName: row.firstName,
    lastName: row.lastName,
  }));
}
async function problem17() {
  const customersWithConditions = await prisma.customer.findMany({
    where: {
      lastName: {
        startsWith: "S",
        contains: "e",
      },
    },
    select: {
      customerID: true,
      firstName: true,
      lastName: true,
      income: true,
      Owns: {
        select: {
          Account: {
            select: {
              balance: true,
            },
          },
        },
      },
    },
    orderBy: {
      customerID: "asc",
    },
  });

  return customersWithConditions
    .filter((row) => row.Owns.length >= 3)
    .map((row) => ({
      customerID: row.customerID,
      firstName: row.firstName,
      lastName: row.lastName,
      income: row.income,
      "average account balance":
        row.Owns.reduce((acc, curr) => acc + Number(curr.Account.balance), 0) /
        row.Owns.length,
    }));
}
async function problem18() {
  const berlinAccounts = await prisma.account.findMany({
    where: {
      Branch: {
        branchName: "Berlin",
      },
    },
    select: {
      accNumber: true,
      balance: true,
      Transactions: {
        select: {
          amount: true,
        },
      },
    },
  });

  const tmp = berlinAccounts
    .filter((row) => row.Transactions.length >= 10)
    .map((row) => ({
      accNumber: row.accNumber,
      balance: row.balance,
      "sum of transaction amounts": row.Transactions.reduce(
        (acc, curr) => acc + Number(curr.amount),
        0
      ),
    }));

  tmp.sort(
    (a, b) => a["sum of transaction amounts"] - b["sum of transaction amounts"]
  );

  return tmp.slice(0, 10);
}

const ProblemList = [
  0,
  problem1,
  problem2,
  problem3,
  // prob4 다시
  problem4,
  problem5,
  problem6,
  problem7,
  problem8,
  problem9,
  problem10,
  problem11,
  12,
  13,
  problem14,
  problem15,
  16,
  problem17,
  problem18,
  19,
  20,
];

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/problems/:num", async (req, res) => {
  const num = Number(req.params.num);

  const result = await ProblemList[num]();
  console.log(`my answer for problem ${num} is ${result}`);

  res.json(result);
});

app.post("/employee/join", async (req, res) => {
  const { firstName, lastName, branchName, salary } = req.body;

  const branchNumber = await prisma.branch.findFirst({
    where: {
      branchName: branchName,
    },
    select: {
      branchNumber: true,
    },
  });
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
    },
  });

  const sins = employees.map((row) => row.sin);

  if (!branchNumber.branchNumber) {
    res.send("wrong branchname");
  }

  const data = {
    sin: Math.max(...sins) + 1,
    firstName: firstName,
    lastName: lastName,
    salary: Number(salary),
    branchNumber: branchNumber.branchNumber,
  };
  console.log(data);

  const create = prisma.employee.create({
    data: data,
  });

  const result = await prisma.$transaction([create]);

  res.send(
    "이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.\n분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다."
  );
});

app.listen(3000, () => {
  console.log(`prisma app listening on port 3000`);
});

app.post("/employee/leave", async (req, res) => {
  const { sin } = req.body;

  const deletion = prisma.employee.delete({
    where: {
      sin: Number(sin),
    },
  });

  const result = await prisma.$transaction([deletion]);

  if (result) {
    res.send(`안녕히 계세요 여러분!
    전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!
    여러분도 행복하세요~~!`);
  } else {
    res.send("탈출 실패!");
  }
});

app.post("/account/:account_no/deposit", async (req, res) => {
  const account_no = Number(req.params.account_no);
  const { customerID, cash } = req.body;

  const accountOwner = await prisma.account.findFirst({
    where: {
      accNumber: Number(account_no),
      Owns: {
        some: {
          customerID: Number(customerID),
        },
      },
    },
    select: {
      balance: true,
    },
  });

  const deposit = prisma.account.update({
    where: {
      accNumber: Number(account_no),
      Owns: {
        some: {
          customerID: Number(customerID),
        },
      },
    },
    data: {
      balance: (Number(accountOwner.balance) + Number(cash)).toString(),
    },
  });

  const result = await prisma.$transaction([deposit]);

  res.send(result.balance);
});

app.post("/account/:account_no/withdraw", async (req, res) => {
  const account_no = Number(req.params.account_no);
  const { customerID, cash } = req.body;

  const accountOwner = await prisma.account.findFirst({
    where: {
      accNumber: Number(account_no),
      Owns: {
        some: {
          customerID: Number(customerID),
        },
      },
    },
    select: {
      balance: true,
    },
  });

  const deposit = prisma.account.update({
    where: {
      accNumber: Number(account_no),
      Owns: {
        some: {
          customerID: Number(customerID),
        },
      },
    },
    data: {
      balance: (Number(accountOwner.balance) - Number(cash)).toString(),
    },
  });

  const check = prisma.account.findFirst({
    where: {
      accNumber: Number(account_no),
      Owns: {
        some: {
          customerID: Number(customerID),
        },
      },
      balance: {
        gte: "",
      },
    },
    select: {
      balance: true,
    },
  });

  const result = await prisma.$transaction([deposit, check]);

  res.send(result.balance.toString());
});
