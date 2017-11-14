//
// Console app to build a EvenSplit Database
//
// EvenSplit is an app to help groups of people
// to keep track of shared expenses and evenly
// split the cost of them.
//
// Viren Mody
// U. of Illinois, Chicago
// CS480, Summer 2016
// Final Project
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace BuildEvenSplit
{
	class Program
	{

		/// <summary>
		/// Executes the given SQL string, which should be an "action" such as 
		/// create table, drop table, insert, update, or delete.  Returns 
		/// normally if successful, throws an exception if not.
		/// </summary>
		/// <param name="sql">query to execute</param>
		static void ExecuteActionQuery(SqlConnection db, string sql)
		{
			SqlCommand cmd = new SqlCommand();
			cmd.Connection = db;
			cmd.CommandText = sql;

			cmd.ExecuteNonQuery();
		}

		static void Main(string[] args)
		{
			Console.WriteLine();
			Console.WriteLine("** Build EvenSplit Database Console App **");
			Console.WriteLine();

			//
			// remote Azure cloud connection info:
			//
			string NetID = "vmody2";
			string databasename = "EvenSplitDB";
			string username = "vmody2";
			string pwd = "VirenCS480";
			
			string connectionInfo = String.Format(@"
Server=tcp:{0}.database.windows.net,1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;
", NetID, databasename, username, pwd);

			// Local DB connection info
			//string connectionInfo = String.Format(@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=|DataDirectory|\DIVVY.mdf;Integrated Security=True;");

			var db = new SqlConnection(connectionInfo);

			string sql;

			try
			{
				//
				// 1. Open DB:
				//
				Console.Write("Opening Azure Database connection: ");

				db.Open();

				Console.WriteLine(db.State);

				//
				// 2. Create Tables:
				//
				Console.WriteLine("Creating tables...");

				sql = string.Format(@"
				-- DROP TABLES (UNCOMMENT TO RUN) --
DROP TABLE ExpenseHistory;
DROP TABLE ExpenseCategories;
DROP TABLE GroupUsers;
DROP TABLE Groups;
DROP TABLE Users;

				-- CREATE TABLE: Users --
CREATE TABLE Users (
	UserID			INT IDENTITY(1001, 1) PRIMARY KEY,
	FirstName		VARCHAR(64) NOT NULL,
	LastName		VARCHAR(64) NOT NULL,
	Phone				BIGINT NOT NULL UNIQUE CHECK (Phone > 999999999 AND Phone < 10000000000),
	Email				NVARCHAR(128) NOT NULL UNIQUE,
	Password		NVARCHAR(64) NOT NULL,
	DateJoined	DATE NOT NULL
);

				-- CREATE TABLE: Groups --
CREATE TABLE Groups (
	GroupID			INT IDENTITY(201, 1) PRIMARY KEY,
	Name				NVARCHAR(128) NOT NULL,
	--NumPeople		INT NOT NULL DEFAULT 1 CHECK (NumPeople >= 1) -- Number of People in Group when created is 1
);
				
				-- CREATE TABLE: GroupUsers --
CREATE TABLE GroupUsers (
	GroupID			INT NOT NULL FOREIGN KEY REFERENCES Groups(GroupID) ON DELETE CASCADE,
	UserID			INT NOT NULL FOREIGN KEY REFERENCES Users(UserID) ON DELETE CASCADE,
	NumResponsibleFor	INT NOT NULL DEFAULT 1 CHECK (NumResponsibleFor >= 1) -- Number of people this user is responsible for in this group,
	PRIMARY KEY (GroupID, UserID)
);

				-- CREATE TABLE: ExpenseCategories --
CREATE TABLE ExpenseCategories (
	CategoryID	TINYINT IDENTITY(1, 1) PRIMARY KEY CHECK (CategoryID <= 9),
	Category		NVARCHAR(30) NOT NULL UNIQUE
);

				-- CREATE TABLE: ExpenseHistory --
CREATE TABLE ExpenseHistory (
	ExpenseID		INT	IDENTITY(30001, 1) PRIMARY KEY,
	GroupID			INT NOT NULL FOREIGN KEY REFERENCES Groups(GroupID) ON DELETE CASCADE,
	UserID			INT NOT NULL FOREIGN KEY REFERENCES Users(UserID) ON DELETE CASCADE,
	CategoryID	TINYINT FOREIGN KEY REFERENCES ExpenseCategories(CategoryID) ON DELETE SET NULL,
	Description	NVARCHAR(256),
	Cost				DECIMAL(9,2) NOT NULL CHECK(Cost >= 0.0),
	Date				DATE NOT NULL
);
");

				ExecuteActionQuery(db, sql);

				//
				// 3. Insert Data
				//
				Console.WriteLine("Inserting data...");

				Console.WriteLine("  Users...");
				string filename = "Users.csv";

				using (var file = new System.IO.StreamReader(filename))
				{
					bool firstline = true;

					while (!file.EndOfStream)
					{
						string line = file.ReadLine();

						if (firstline)  // skip first line (header row):
						{
							firstline = false;
							continue;
						}

						string[] values = line.Split(',');

						//int userid = Convert.ToInt32(values[0]);
						string firstName = values[1];
						string lastName = values[2];
						long phone = Convert.ToInt64(values[3]);
						string email = values[4];
						string password = values[5];
						DateTime dateJoined = Convert.ToDateTime(values[6]);

						sql = string.Format(@"
INSERT INTO 
  Users(FirstName, LastName, Phone, Email, Password, DateJoined)
  Values('{0}', '{1}', {2}, '{3}', '{4}', '{5}');
", firstName, lastName, phone, email, password, dateJoined.ToShortDateString());

						ExecuteActionQuery(db, sql);
					}//while
				}//using



				Console.WriteLine("  Groups...");
				filename = "Groups.csv";

				using (var file = new System.IO.StreamReader(filename))
				{
					bool firstline = true;

					while (!file.EndOfStream)
					{
						string line = file.ReadLine();

						if (firstline)  // skip first line (header row):
						{
							firstline = false;
							continue;
						}

						string[] values = line.Split(',');

						//int groupid = Convert.ToInt32(values[0]);
						string groupname = values[1];
						//int numpeople = Convert.ToInt32(values[2]);

						sql = string.Format(@"
INSERT INTO 
  Groups(Name)
  Values('{0}');
", groupname);

						ExecuteActionQuery(db, sql);
					}//while
				}//using



				Console.WriteLine("  GroupUsers...");
				filename = "GroupUsers.csv";

				using (var file = new System.IO.StreamReader(filename))
				{
					bool firstline = true;

					while (!file.EndOfStream)
					{
						string line = file.ReadLine();

						if (firstline)  // skip first line (header row):
						{
							firstline = false;
							continue;
						}

						string[] values = line.Split(',');

						int groupid = Convert.ToInt32(values[0]);
						int userid = Convert.ToInt32(values[1]);
						int numpeople = Convert.ToInt32(values[2]);

						sql = string.Format(@"
INSERT INTO 
  GroupUsers(GroupID, UserID, NumResponsibleFor)
  Values({0}, {1}, {2});
", groupid, userid, numpeople);

						ExecuteActionQuery(db, sql);
					}//while
				}//using



				Console.WriteLine("  ExpenseCategories...");
				filename = "ExpenseCategories.csv";

				using (var file = new System.IO.StreamReader(filename))
				{
					bool firstline = true;

					while (!file.EndOfStream)
					{
						string line = file.ReadLine();

						if (firstline)  // skip first line (header row):
						{
							firstline = false;
							continue;
						}

						string[] values = line.Split(',');

						//int categoryid = Convert.ToInt32(values[0]);
						string category = values[1];

						sql = string.Format(@"
INSERT INTO 
  ExpenseCategories(Category)
  Values('{0}');
", category);

						ExecuteActionQuery(db, sql);
					}//while
				}//using



				Console.WriteLine("  ExpenseHistory...");
				filename = "ExpenseHistory.csv";

				using (var file = new System.IO.StreamReader(filename))
				{
					bool firstline = true;

					while (!file.EndOfStream)
					{
						string line = file.ReadLine();

						if (firstline)  // skip first line (header row):
						{
							firstline = false;
							continue;
						}

						string[] values = line.Split(',');

						//int expenseid = Convert.ToInt32(values[0]);
						int groupid = Convert.ToInt32(values[1]);
						int userid = Convert.ToInt32(values[2]);
						int categoryid = Convert.ToInt32(values[3]);
						string description = values[4];
						double cost = Convert.ToDouble(values[5]);
						DateTime date = Convert.ToDateTime(values[6]);

						sql = string.Format(@"
INSERT INTO 
  ExpenseHistory(GroupID, UserID, CategoryID, Description, Cost, Date)
	Values({0}, {1}, {2}, '{3}', {4}, '{5}');
", groupid, userid, categoryid, description, cost, date.ToShortDateString());

						ExecuteActionQuery(db, sql);
					}//while
				}//using

				//
				// 4. Create Stored Procedures
				//
				Console.WriteLine("Creating stored procedures...");

				Console.WriteLine("  AddExpense...");
				//
				// Stored Procedure: Add Expense
				// 

				sql = string.Format(@"
CREATE PROCEDURE AddExpense
	@email				NVARCHAR(64),
	@groupname		NVARCHAR(64),
	@category			NVARCHAR(64),
	@description	NVARCHAR(256),
	@cost					DECIMAL(9,2),
	@date					DATE
AS
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION;

	DECLARE @uid AS INT;
	DECLARE @gid AS INT;
	DECLARE @cid AS INT;

	-- CHECK IF EMAIL EXISTS --
	SELECT @uid = UserID
	FROM Users
	WHERE Email = @email;

	IF @uid IS NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -1;
	END

	-- CHECK IF GROUP EXISTS --
	SELECT @gid = GroupID
	FROM Groups
	WHERE Name = @groupname;

	IF @gid is NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -2;
	END

	-- CHECK IF CATEGORY EXISTS --
	SELECT @cid = CategoryID
	FROM ExpenseCategories
	WHERE Category = @category;

	IF @cid IS NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -3;
	END

	-- ALL IS WELL: ADD/INSERT EXPENSE --
	INSERT INTO
		ExpenseHistory(GroupID, UserID, CategoryID, Description, Cost, Date)
		Values(@gid, @uid, @cid, @description, @cost, @date);
	
	IF @@ERROR <> 0 OR @@ROWCOUNT <> 1
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -4;
	END

	COMMIT TRANSACTION;
	RETURN 0;
");
				ExecuteActionQuery(db, sql);

				//
				// Stored Procedure: Delete Expense
				// 
				Console.WriteLine("  DeleteExpense...");

				sql = string.Format(@"
CREATE PROCEDURE DeleteExpense
	@eid				INT
AS
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION;

	DECLARE @expID AS INT;

	-- CHECK IF EXPENSE STILL EXISTS --
	SELECT @expID = UserID
	FROM ExpenseHistory
	WHERE ExpenseID = @eid;

	IF @expID IS NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -1;
	END

	-- ALL IS WELL: DELETE EXPENSE --
	DELETE FROM ExpenseHistory
	WHERE ExpenseID = @eid;
	
	IF @@ERROR <> 0 OR @@ROWCOUNT <> 1
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -2;
	END

	COMMIT TRANSACTION;
	RETURN 0;
");
				ExecuteActionQuery(db, sql);


				//
				// Stored Procedure: Add Group
				// 
				Console.WriteLine("  AddGroup...");

				sql = string.Format(@"
CREATE PROCEDURE AddGroup
	@groupname				NVARCHAR(64),
	@email						NVARCHAR(64),
	@numpeople				INT
AS
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION;

	DECLARE @uid AS INT;
	DECLARE @useremail AS NVARCHAR(64);
	DECLARE @grpname AS NVARCHAR(64);
	DECLARE @newgid AS INT;

	-- CHECK IF EMAIL EXISTS --
	SELECT @uid = UserID
	FROM Users
	WHERE Email = @email;

	IF @uid IS NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -1;
	END

	-- CHECK IF GROUPNAME ALREADY EXISTS FOR THAT USER EMAIL --
	SELECT @grpname = Name
	FROM Groups
	INNER JOIN GroupUsers
	ON Groups.GroupID = GroupUsers.GroupID
	INNER JOIN Users
	ON GroupUsers.UserID = Users.UserID
	WHERE Email = @email AND Name = @groupname;

	IF @grpname IS NOT NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -2;
	END

	-- ALL IS WELL: INSERT GROUP --
	INSERT INTO
		Groups(Name)
		Values(@groupname);

	SET @newgid = @@IDENTITY;
	
	IF @@ERROR <> 0 OR @@ROWCOUNT <> 1
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -3;
	END

	INSERT INTO
		GroupUsers(GroupID, UserID, NumResponsibleFor)
		Values(@newgid, @uid, @numpeople);
	
	IF @@ERROR <> 0 OR @@ROWCOUNT <> 1
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -3;
	END

	COMMIT TRANSACTION;
	RETURN 0;
");
				ExecuteActionQuery(db, sql);


				//
				// Stored Procedure: Add Member to a Group
				// 
				Console.WriteLine("  AddMember...");

				sql = string.Format(@"
CREATE PROCEDURE AddMember
	@groupname				NVARCHAR(64),
	@email						NVARCHAR(64),
	@membernumpeople	INT
AS
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION;

	DECLARE @uid AS INT;
	DECLARE @gid AS INT;
	DECLARE @useremail AS NVARCHAR(64);

	-- CHECK IF EMAIL EXISTS --
	SELECT @uid = UserID
	FROM Users
	WHERE Email = @email;

	IF @uid IS NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -1;
	END

	-- CHECK IF EMAIL ALREADY EXISTS IN THAT GROUP --
	SELECT @useremail = Email
	FROM Users
	INNER JOIN GroupUsers
	ON Users.UserID = GroupUsers.UserID
	INNER JOIN GROUPS
	ON GroupUsers.GroupID = Groups.GroupID
	WHERE Groups.Name = @groupname AND Email = @email;
	
	IF @useremail IS NOT NULL
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -2;
	END

	-- ALL IS WELL: INSERT MEMBER --
	-- GET GroupID FOR INSERT --
	SELECT @gid = GroupID
	FROM Groups
	WHERE Name = @groupname;

	INSERT INTO
		GroupUsers(GroupID, UserID, NumResponsibleFor)
		Values(@gid, @uid, @membernumpeople);

	IF @@ERROR <> 0 OR @@ROWCOUNT <> 1
	BEGIN
		ROLLBACK TRANSACTION;
		RETURN -3;
	END
	
	COMMIT TRANSACTION;
	RETURN 0;
");
				ExecuteActionQuery(db, sql);

			}
			catch (Exception ex)
			{
				Console.WriteLine("**Exception: '{0}'", ex.Message);
			}
			finally
			{
				Console.Write("Closing database connection: ");

				if (db != null && db.State == ConnectionState.Open)
					db.Close();

				Console.WriteLine(db.State);
			}

			Console.WriteLine();
			Console.WriteLine("** Done **");
			Console.WriteLine();
		}//main

	}//class

}//namespace
