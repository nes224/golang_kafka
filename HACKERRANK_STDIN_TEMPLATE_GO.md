# HackerRank OA — อ่าน stdin (Go) ครบทุกแบบ

ปัญหาที่เจอ: LeetCode ให้ argument มาเลย · HackerRank โยน **text ดิบทาง stdin** ต้อง parse เอง
→ parse ผิด = 0 case ทั้งที่ algorithm ถูก · **ท่อง template นี้ใช้ได้ทุกข้อ**

---

## 0. เครื่องมือ 4 ตัว (จำแค่นี้)
| ตัว | ทำอะไร |
|---|---|
| `bufio.NewScanner(os.Stdin)` | ตัวอ่าน (เร็ว) |
| `strings.Fields(s)` | แยก string ตามช่องว่าง → `[]string` |
| `strconv.Atoi(s)` | string → int (`ParseInt/ParseFloat` สำหรับแบบอื่น) |
| `fmt.Println / Printf` | พิมพ์ผลออก stdout |

> ⚠️ ใช้ `bufio.Scanner` ต้อง **เพิ่ม buffer** ถ้า input ใหญ่ (ไม่งั้นอ่านขาด):
> ```go
> sc := bufio.NewScanner(os.Stdin)
> sc.Buffer(make([]byte, 1024*1024), 1024*1024)  // 1MB
> ```

---

## 1. Setup มาตรฐาน (วางหัวไฟล์ทุกครั้ง)
```go
package main

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
)

var sc = bufio.NewScanner(os.Stdin)
var w  = bufio.NewWriter(os.Stdout)

func readLine() string { sc.Scan(); return sc.Text() }
func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// อ่านบรรทัดที่เป็นตัวเลขหลายตัว → []int
func readInts() []int {
    fields := strings.Fields(readLine())
    a := make([]int, len(fields))
    for i, f := range fields { a[i] = atoi(f) }
    return a
}

func main() {
    sc.Buffer(make([]byte, 1024*1024), 1024*1024)
    sc.Split(bufio.ScanLines)
    defer w.Flush()

    // ... code ...
}
```

---

## 2. แบบ input ที่เจอบ่อย (copy ไปใช้)

### แบบ A — บรรทัดเดียว 2 ค่า (เช่น n, target)
```
3 6
```
```go
first := strings.Fields(readLine())
n, target := atoi(first[0]), atoi(first[1])
```

### แบบ B — บรรทัด n แล้วบรรทัด array
```
3
2 7 11
```
```go
n := atoi(readLine())
nums := readInts()
_ = n
```

### แบบ C — array มาทีละบรรทัด (n บรรทัด)
```
3
2
7
11
```
```go
n := atoi(readLine())
nums := make([]int, n)
for i := 0; i < n; i++ {
    nums[i] = atoi(readLine())
}
```

### แบบ D — หลาย test case (T รอบ) ★ เจอบ่อยสุดใน OA
```
2            ← T = 2 รอบ
3 6
2 7 11
2 5
1 4
```
```go
t := atoi(readLine())
for ; t > 0; t-- {
    hdr := strings.Fields(readLine())
    n, target := atoi(hdr[0]), atoi(hdr[1])
    nums := readInts()
    ans := solve(n, target, nums)
    fmt.Fprintln(w, ans)      // พิมพ์ผลแต่ละรอบ
}
```

### แบบ E — matrix / grid (r x c)
```
2 3
1 2 3
4 5 6
```
```go
dim := strings.Fields(readLine())
r, c := atoi(dim[0]), atoi(dim[1])
grid := make([][]int, r)
for i := 0; i < r; i++ {
    grid[i] = readInts()
}
_ = c
```

### แบบ F — string / คำ
```
hello world
```
```go
line := readLine()             // "hello world"
words := strings.Fields(line)  // ["hello","world"]
```

---

## 3. พิมพ์ผลออก (output)
```go
fmt.Fprintln(w, ans)                    // 1 ค่า/บรรทัด (w = buffered writer)
fmt.Fprintln(w, a, b, c)                // หลายค่าเว้นวรรค
// array ออกเว้นวรรค:
strs := make([]string, len(arr))
for i, v := range arr { strs[i] = strconv.Itoa(v) }
fmt.Fprintln(w, strings.Join(strs, " "))
```
> อย่าลืม `defer w.Flush()` ไม่งั้น output ไม่ออก!

---

## 4. checklist กันพลาด OA
- [ ] อ่าน format input ให้ชัดก่อนเขียน (กี่บรรทัด? มี T test case ไหม?)
- [ ] เพิ่ม buffer ถ้า input อาจใหญ่
- [ ] `defer w.Flush()` เสมอ (ถ้าใช้ buffered writer)
- [ ] output format ตรงเป๊ะ (เว้นวรรค/ขึ้นบรรทัด/ตัวพิมพ์) — ผิดนิดเดียว = case ไม่ผ่าน
- [ ] test ด้วย sample input ที่โจทย์ให้ก่อน submit

---

## 5. วิธีซ้อม (1-2 ชม. ก็คล่อง)
1. เข้า HackerRank → หมวด **"Warmup"** หรือ **"Input/Output"** ทำ 5-6 ข้อ (ง่ายมาก แต่ฝึก parse)
2. ท่องแบบ D (หลาย test case) กับ E (matrix) ให้ขึ้นใจ — เจอบ่อยสุด
3. พอ parse คล่องแล้ว → เอา algorithm ที่รู้อยู่แล้วมาใส่ = คะแนนพุ่ง

> สรุป: 10/60 รอบที่แล้ว **ส่วนใหญ่เสียที่ parse ไม่ใช่ algorithm** · ปิดช่องนี้ได้ = พลิกเกมทันที
