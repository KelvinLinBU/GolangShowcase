package bank

import (
	"log"
	"sync"
)

// to see if code is being updated?
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Bank represents a simple banking system
type Bank struct {
	bankLock *sync.Mutex
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}

// initializes a new bank
func BankInit() *Bank {
	b := Bank{&sync.Mutex{}, make(map[int]*Account)}
	return &b
}

func (b *Bank) notifyAccountHolder(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) notifySupportTeam(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) logInsufficientBalanceEvent(accountID int) {
	DPrintf("Insufficient balance in account %d for withdrawal\n", accountID)
}

func (b *Bank) sendEmailTo(accountID int) {
	// Dummy function
	// hello
	// marker
	// eraser
}

// creates a new account with the given account ID
func (b *Bank) CreateAccount(accountID int) { //Works
	// your code here
	b.bankLock.Lock()         //Lock the code
	defer b.bankLock.Unlock() //unlock the code afterwards
	if _, ok := b.accounts[accountID]; ok {
		panic("account already exists")
	} else {
		account_details := &Account{
			balance: 0,               //initial balance is 0
			lock:    new(sync.Mutex), //mutex
		}
		b.accounts[accountID] = account_details
	}
	//gets b.accounts map, and sets account_id as the key and account as the value
}

// deposit a given amount to the specified account
func (b *Bank) Deposit(accountID, amount int) { //Done

	b.bankLock.Lock()

	account, ok := b.accounts[accountID]
	b.bankLock.Unlock()
	if ok {

		account.lock.Lock()

		DPrintf("[ACQUIRED LOCK][DEPOSIT] for account %d\n", accountID)
		newBalance := account.balance + amount
		account.balance = newBalance
		account.lock.Unlock() //need to unlock the account after done
		DPrintf("Deposited %d into account %d. New balance: %d\n", amount, accountID, newBalance)

		//b.accounts[accountID].lock.Unlock()
		DPrintf("[RELEASED LOCK][DEPOSIT] for account %d\n", accountID)
	} else {
		panic("account does not exist")
	}

}

// withdraw the given amount from the given account id
func (b *Bank) Withdraw(accountID, amount int) bool { //Done
	b.bankLock.Lock() //code copy pasted from deposit
	account, ok := b.accounts[accountID]
	b.bankLock.Unlock()
	DPrintf("[ACQUIRED LOCK][WITHDRAW] for account %d\n", accountID)
	if ok {
		account.lock.Lock() //moved account.lock.Lock() down here
		if account.balance >= amount {

			newBalance := account.balance - amount
			account.balance = newBalance
			DPrintf("Withdrawn %d from account %d. New balance: %d\n", amount, accountID, newBalance)
			//account.lock.Unlock()
			account.lock.Unlock() //need to unlock the account after done
			DPrintf("[RELEASED LOCK][WITHDRAW] for account %d\n", accountID)
			//fmt.Println("Yes Money")
			return true
		} else {
			account.lock.Unlock()
			// Insufficient balance in account %d for withdrawal
			// Please contact the account holder or take appropriate action.
			// trigger a notification or alert mechanism
			b.notifyAccountHolder(accountID)
			b.notifySupportTeam(accountID)
			// log the event for further investigation
			b.logInsufficientBalanceEvent(accountID)
			//fmt.Println("No money")
			return false
		}
	} else {
		panic("account does not exist")
		//return false
	}
}

// transfer amount from sender to receiver
func (b *Bank) Transfer(sender int, receiver int, amount int, allowOverdraw bool) bool {
	success := false

	b.bankLock.Lock()
	senderAccount, ok1 := b.accounts[sender] //get the two accounts
	receiverAccount, ok2 := b.accounts[receiver]
	b.bankLock.Unlock()
	if ok1 && ok2 { //if both accounts exist
		var first_account, second_account *Account
		if sender < receiver {
			first_account, second_account = senderAccount, receiverAccount
		} else {
			first_account, second_account = receiverAccount, senderAccount
		}
		first_account.lock.Lock()
		second_account.lock.Lock()
		if (senderAccount.balance >= amount) || allowOverdraw {
			//fmt.Println("this case: ")
			variable := senderAccount.balance - amount
			//fmt.Println("Hello")
			variable2 := receiverAccount.balance + amount
			//fmt.Println("Jello")
			senderAccount.balance = variable
			//fmt.Println("Nello")
			receiverAccount.balance = variable2
			//fmt.Println("Vello")
			success = true
		}
		senderAccount.lock.Unlock()
		receiverAccount.lock.Unlock()
		// if the sender has enough balance,
		// or if overdraws are allowed
		return success
	} else {
		panic("account does not exist")
		//return false
	}
}

func (b *Bank) DepositAndCompare(accountId int, amount int, compareThreshold int) bool {

	b.bankLock.Lock()
	account, ok := b.accounts[accountId]
	if ok {
		b.bankLock.Unlock()
		b.Deposit(accountId, amount) //deposit first
		account.lock.Lock()
		new_balance := account.balance
		account.lock.Unlock()
		//defer account.lock.Unlock() unnecessary locks
		compareResult := (new_balance >= compareThreshold)
		// return compared result
		return compareResult
	} else {
		b.bankLock.Unlock()
		panic("account does not exist")
		//return false
	}
}

// returns the balance of the given account id
func (b *Bank) GetBalance(accountID int) int {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()
	account.lock.Lock()
	defer account.lock.Unlock()
	return account.balance
}


