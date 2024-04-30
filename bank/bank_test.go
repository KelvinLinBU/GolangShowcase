package bank

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func CreateAccounts(accountCount int, bank *Bank) {
	// Create a lot of accounts
	var wg sync.WaitGroup
	for i := 0; i < accountCount; i++ {
		wg.Add(1)
		go func(accountId int) {
			bank.CreateAccount(accountId)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestCreateAccountBasic(t *testing.T) {
	accountCount := 1
	bank := BankInit()
	CreateAccounts(accountCount, bank)
	if len(bank.accounts) != accountCount {
		t.Errorf("Expected %d account, got %d\n", accountCount, len(bank.accounts))
	}
}

func TestCreateAccountMany(t *testing.T) {
	accountCount := 100
	bank := BankInit()
	CreateAccounts(accountCount, bank)
	if len(bank.accounts) != accountCount {
		t.Errorf("Expected %d account, got %d\n", accountCount, len(bank.accounts))
	}
}

func TestManyDepositsAndWithdraws(t *testing.T) {
	timeoutTime := 10
	accountCount := 100
	totalOperations := 10000
	bank := BankInit()
	CreateAccounts(accountCount, bank)
	timeout := time.After(time.Duration(timeoutTime) * time.Second)
	done := make(chan bool)
	balancesMutex := &sync.Mutex{}
	balancesForVerification := make([]int, accountCount)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < totalOperations; i++ {
			for accountId := 0; accountId < accountCount; accountId++ {
				// Simultaneous deposit and withdrawals
				if rand.Intn(42)%2 == 0 {
					// withdraw
					availableBalance := bank.GetBalance(accountId)
					withdrawAmount := availableBalance / 2
					// to overdraw or not to overdraw?
					if rand.Intn(100) < 20 {
						// overdraw flow
						// try to withdraw twice the balance
						withdrawAmount = withdrawAmount * 4
					}
					wg.Add(1)
					go func(accountId int) {
						success := bank.Withdraw(accountId, withdrawAmount)
						if success {
							// wg.Add(1)
							func() {
								balancesMutex.Lock()
								balancesForVerification[accountId] -= withdrawAmount
								balancesMutex.Unlock()
								// wg.Done()
							}()
						}
						wg.Done() // edited because waitgroup should not be required for withdraws/deposit
					}(accountId)
				} else {
					// deposit
					depositAmount := 100
					wg.Add(1)
					go func(accountId int) {
						bank.Deposit(accountId, depositAmount)
						// wg.Add(1)
						func() {
							balancesMutex.Lock()
							balancesForVerification[accountId] += depositAmount
							balancesMutex.Unlock()
							// wg.Done()
						}()
						wg.Done() // edited because waitgroup should not be required for withdraws/deposit
					}(accountId)
				}
			}
		}
		// Wait for all operations to complete
		wg.Wait()
		done <- true
	}()

	select {
	case <-timeout:
		t.Error("Test didn't finish in time (10 seconds)")
	case <-done:
		balancesMutex.Lock()
		// fmt.Println("Final Balances")
		// fmt.Println(balancesForVerification)
		for i := 0; i < accountCount; i++ {
			// fmt.Printf("%d  ---  %d\n", balancesForVerification[i], bank.GetBalance(i))
			// fmt.Println(balancesForVerification)
			if balancesForVerification[i] != bank.GetBalance(i) {
				t.Error("Final balances do not match!\n")
			}
		}
		balancesMutex.Unlock()
		// t.Logf("Test finished under %d seconds\n", timeoutTime)
	}
}

func TestFewTransfers(t *testing.T) {
	timeoutTime := 2
	// 2 accounts 40 transfers each
	transferCount := 40
	bank := BankInit()
	bank.CreateAccount(1)
	bank.CreateAccount(0) // zero indexed, cuz accounts is an array
	bank.Deposit(0, 100)
	bank.Deposit(1, 100)
	done := make(chan bool)
	// fmt.Println("TestFewTransfers begins")
	// fmt.Printf("Old balances, got %d %d \n",
	// bank.GetBalance(0), bank.GetBalance(1))
	timeout := time.After(time.Duration(timeoutTime) * time.Second)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < transferCount; i++ {
			wg.Add(2)
			go func() {
				bank.Transfer(0, 1, 2, false)
				wg.Done()
			}()
			go func() {
				bank.Transfer(1, 0, 1, false)
				wg.Done()
			}()
		}
		// Wait for all operations to complete
		wg.Wait()
		done <- true
	}()
	select {
	case <-timeout:
		t.Error("TestFewTransfers failed to finish in time")
	case <-done:
		if bank.GetBalance(0) == 100-transferCount && bank.GetBalance(1) == 100+transferCount {
			// fmt.Println("TestFewTransfers finished.")
		} else {
			t.Errorf("TestFewTransfers failed due to incorrect balances, got %d %d \n",
				bank.GetBalance(0), bank.GetBalance(1))
		}
		// fmt.Printf("Final balances, got %d %d \n",
		// bank.GetBalance(0), bank.GetBalance(1))
	}
}

func TestManyTransfers(t *testing.T) {
	timeoutTime := 4
	// 2 accounts 40 transfers each
	transferCount := 400
	transferAmount := 2
	bank := BankInit()
	accountCount := 8

	balancesMutex := &sync.Mutex{}
	balancesForVerification := make([]int, accountCount)

	// give everyone some $$$
	CreateAccounts(accountCount, bank)
	for accountId := 0; accountId < accountCount; accountId++ {
		bank.Deposit(accountId, 1000)
		balancesMutex.Lock()
		balancesForVerification[accountId] = 1000
		balancesMutex.Unlock()
	}

	done := make(chan bool)
	// fmt.Println("TestManyTransfer begins")
	// fmt.Printf("Old balances, got %d %d \n",
	// bank.GetBalance(0), bank.GetBalance(1))
	timeout := time.After(time.Duration(timeoutTime) * time.Second)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < transferCount; i++ {
			for sender := 0; sender < accountCount; sender++ {
				for receiver := 0; receiver < accountCount; receiver++ {

					// dont transfer to yourself >.<
					if sender == receiver {
						continue
					}
					wg.Add(1)
					go func(s int, r int, amount int) {
						success := bank.Transfer(s, r, 2, false)
						if success {
							func() {
								balancesMutex.Lock()
								balancesForVerification[s] -= transferAmount
								balancesForVerification[r] += transferAmount
								balancesMutex.Unlock()
							}()
						}
						wg.Done()

					}(sender, receiver, transferAmount)
				}
			}

		}
		// Wait for all operations to complete
		wg.Wait()
		done <- true
	}()
	select {
	case <-timeout:
		t.Error("TestManyTransfer failed to finish in time")
	case <-done:
		balancesMutex.Lock()
		for i := 0; i < accountCount; i++ {
			// fmt.Printf("%d  ---  %d\n", balancesForVerification[i], bank.GetBalance(i))
			if balancesForVerification[i] != bank.GetBalance(i) {
				t.Error("Final balances do not match!\n")
			}
		}
		balancesMutex.Unlock()
	}
}

func TestDepositAndCompare(t *testing.T) {
	timeoutTime := 8
	accountCount := 10
	totalOperations := 100
	bank := BankInit()
	CreateAccounts(accountCount, bank)
	timeout := time.After(time.Duration(timeoutTime) * time.Second)
	resultCh := make(map[int]chan bool)
	countTrue := make(map[int]int)
	countFalse := make(map[int]int)
	threshold := make(map[int]int)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < accountCount; i++ {
		resultCh[i] = make(chan bool, totalOperations+1)
		threshold[i] = rand.Intn(6000) + 2000
	}
	done := make(chan bool)
	err := make(chan bool)

	go func() {
		for i := 0; i < totalOperations; i++ {
			for accountId := 0; accountId < accountCount; accountId++ {
				go func(accountId int, operationNumber int) {
					resultCh[accountId] <- bank.DepositAndCompare(accountId, 100, threshold[accountId])
				}(accountId, i)
			}
		}
		for i := 0; i < accountCount; i++ {
			countTrue[i] = 0
			countFalse[i] = 0
			for j := 0; j < totalOperations; j++ {
				result := <-resultCh[i]
				if result {
					countTrue[i]++
				} else {
					countFalse[i]++
				}
			}
		}
		done <- true
	}()

	select {
	case <-timeout:
		t.Error("Test didn't finish in time")
	case <-done:
		resultCheck := true
		for i := 0; i < accountCount; i++ {
			gt := (threshold[i] - 1) / 100
			if countFalse[i] != gt || countTrue[i] != totalOperations-gt {
				resultCheck = false
			}
		}
		if resultCheck {
			// fmt.Println("Test finished")
		} else {
			t.Error("Compare result incorrect!", countTrue, countFalse)
		}
	// t.Logf("Test finished under %d seconds\n", timeoutTime)
	// TODO: Fix this?
	case <-err:
		t.Error("Compare result incorrect! ", err)
	}

}

func TestHiddenBonus(t *testing.T) {
	timeoutTime := 8
	accountCount := 40
	totalOperations := 8000
	bank := BankInit()
	rand.Seed(time.Now().UnixNano())
	CreateAccounts(accountCount, bank)
	timeout := time.After(time.Duration(timeoutTime) * time.Second)
	done := make(chan bool)
	balancesMutex := &sync.Mutex{}
	balancesForVerification := make([]int, accountCount)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < totalOperations; i++ {
			if i%100 == 40 && i > accountCount {
				for j := i; j < i+40; j++ {
					wg.Add(1)
					go func(accountId int) {
						bank.CreateAccount(accountId)
						wg.Done()
					}(j)
				}
			}
			for accountId := 0; accountId < accountCount; accountId++ {
				randOperation := rand.Intn(42) % 3
				if randOperation == 0 {
					availableBalance := bank.GetBalance(accountId)
					withdrawAmount := availableBalance / 2
					if rand.Intn(100) < 20 {
						withdrawAmount = withdrawAmount * 4
					}
					wg.Add(1)
					go func(accountId int) {
						success := bank.Withdraw(accountId, withdrawAmount)
						if success {
							balancesMutex.Lock()
							balancesForVerification[accountId] -= withdrawAmount
							balancesMutex.Unlock()
						}
						wg.Done()
					}(accountId)
				} else if randOperation == 1 {
					depositAmount := 100
					wg.Add(1)
					go func(accountId int) {
						bank.Deposit(accountId, depositAmount)
						balancesMutex.Lock()
						balancesForVerification[accountId] += depositAmount
						balancesMutex.Unlock()
						wg.Done()
					}(accountId)
				} else {
					sender := accountId
					receiver := rand.Intn(accountCount)
					for receiver == sender {
						receiver = rand.Intn(accountCount)
					}
					amount := 10
					wg.Add(1)
					go func(s int, r int, amount int) {
						success := bank.Transfer(s, r, amount, false)
						if success {
							balancesMutex.Lock()
							balancesForVerification[s] -= amount
							balancesForVerification[r] += amount
							balancesMutex.Unlock()
						}
						wg.Done()
					}(sender, receiver, amount)
				}
			}
		}
		wg.Wait()
		done <- true
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Creating duplicate account didn't panic!")
			}
		}()
		bank.CreateAccount(rand.Intn(accountCount))
	}()
	select {
	case <-timeout:
		t.Error("Test didn't finish in time (10 seconds)")
	case <-done:
		balancesMutex.Lock()
		for i := 0; i < accountCount; i++ {
			if balancesForVerification[i] != bank.GetBalance(i) {
				t.Error("Final balances do not match!\n")
			}
		}
		balancesMutex.Unlock()
	}
}
