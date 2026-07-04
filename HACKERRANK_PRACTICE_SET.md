# HackerRank-style Practice Set (stdin/stdout · Go)

โจทย์เลียนแบบ format HackerRank จริง — โจทย์ยาว + Input/Output Format + Constraints + Sample
**ลองทำเองก่อน** · เฉลย approach อยู่ท้ายไฟล์ (ห้ามแอบดูก่อนพยายาม ~15 นาที)
ซ้อม parse stdin ด้วย template ใน `HACKERRANK_STDIN_TEMPLATE_GO.md`

ลำดับ: P1 (warmup I/O) → P2 (hash) → P3 (stack) → P4 (grid DFS) → P5 (greedy)

---

## P1 — Sum of Even Elements  ★ (warmup: ฝึก multi test case)

มีอาเรย์จำนวนเต็มหลายชุด สำหรับแต่ละชุดให้หา **ผลรวมของเลขที่เป็นจำนวนคู่**

**Input Format**
- บรรทัดแรก: จำนวนเต็ม `T` — จำนวนชุดทดสอบ
- แต่ละชุด 2 บรรทัด: บรรทัดแรก `n` (ขนาดอาเรย์) · บรรทัดสอง `n` จำนวนคั่นด้วยช่องว่าง

**Constraints**
- `1 ≤ T ≤ 100`
- `1 ≤ n ≤ 10^5`
- `-10^9 ≤ a[i] ≤ 10^9`

**Output Format**
- พิมพ์ผลรวมเลขคู่ของแต่ละชุด ชุดละ 1 บรรทัด

**Sample Input**
```
2
4
1 2 3 4
3
5 5 5
```
**Sample Output**
```
6
0
```
**Explanation:** ชุดแรก 2+4=6 · ชุดสองไม่มีเลขคู่ = 0
> ⚠️ ผลรวมอาจเกิน int32 → ใช้ `int` (Go = 64-bit) หรือ `int64`

---

## P2 — Count Pairs with Given Sum  ★★ (hash)

ให้อาเรย์และค่า `k` — นับจำนวน **คู่ (i, j) ที่ i < j และ a[i] + a[j] = k**

**Input Format**
- บรรทัดแรก: `n k`
- บรรทัดสอง: `n` จำนวนคั่นช่องว่าง

**Constraints**
- `1 ≤ n ≤ 10^5`
- `1 ≤ a[i] ≤ 10^9`, `1 ≤ k ≤ 2·10^9`

**Output Format** — จำนวนคู่ (1 บรรทัด)

**Sample Input**
```
5 6
1 5 3 3 3
```
**Sample Output**
```
4
```
**Explanation:** คู่ที่รวมได้ 6 = (1,5) และ (3,3) สามคู่จากเลข 3 สามตัว → รวม 4
> hint: อย่าใช้ 2 loop (O(n²) จะ TLE) · ใช้ map นับความถี่

---

## P3 — Balanced Brackets  ★★ (stack)

ตรวจว่าสตริงวงเล็บ `()[]{}` **สมดุลถูกต้อง** ไหม (เปิด-ปิดครบ + ซ้อนถูกชนิด)

**Input Format**
- บรรทัดแรก: `T` จำนวนสตริง
- ถัดมา `T` บรรทัด: สตริงละบรรทัด

**Constraints**
- `1 ≤ T ≤ 100`, `1 ≤ len ≤ 10^4`

**Output Format** — แต่ละสตริงพิมพ์ `YES` ถ้าสมดุล ไม่งั้น `NO`

**Sample Input**
```
3
{[()]}
{[(])}
{{[[(())]]}}
```
**Sample Output**
```
YES
NO
YES
```
> hint: stack — เจอวงเล็บเปิด push · เจอปิด ต้อง match กับ top

---

## P4 — Connected Cells (Count Regions)  ★★★ (grid DFS/BFS)

ตารางขนาด `r × c` มีค่า 0/1 · หา **จำนวนกลุ่มของเซลล์ 1 ที่ติดกัน** (ติดกันแบบ 8 ทิศ: บน/ล่าง/ซ้าย/ขวา/ทแยง)

**Input Format**
- บรรทัดแรก: `r c`
- ถัดมา `r` บรรทัด: แต่ละบรรทัด `c` จำนวน (0/1) คั่นช่องว่าง

**Constraints** — `1 ≤ r, c ≤ 100`

**Output Format** — จำนวนกลุ่ม (1 บรรทัด)

**Sample Input**
```
4 4
1 1 0 0
0 1 1 0
0 0 1 0
1 0 0 0
```
**Sample Output**
```
2
```
**Explanation:** กลุ่มบน-กลาง (1 ต่อกัน 5 ตัว) = 1 กลุ่ม · เซลล์ 1 มุมล่างซ้าย = อีก 1 กลุ่ม → 2
> hint: วน grid ทุกช่อง · เจอ 1 ที่ยังไม่ visit → count++ แล้ว DFS/BFS ลบทั้งกลุ่ม (8 ทิศ)

---

## P5 — Jumping on the Clouds  ★★★ (greedy)

เดินบนก้อนเมฆเรียงกัน `c[i]`: 0 = ปลอดภัย, 1 = ห้ามเหยียบ · เริ่มที่ index 0 ต้องไปให้ถึง index `n-1`
แต่ละก้าวกระโดดได้ **1 หรือ 2 ช่อง** (ห้ามลงก้อน 1) · หา **จำนวนก้าวน้อยสุด**

**Input Format**
- บรรทัดแรก: `n`
- บรรทัดสอง: `n` จำนวน (0/1) คั่นช่องว่าง (รับประกันว่าไปถึงได้)

**Constraints** — `2 ≤ n ≤ 100`, `c[0]=c[n-1]=0`

**Output Format** — จำนวนก้าวน้อยสุด (1 บรรทัด)

**Sample Input**
```
7
0 0 1 0 0 1 0
```
**Sample Output**
```
4
```
**Explanation:** 0→1→3→4→6 = 4 ก้าว (กระโดดข้าม 1 ทุกครั้งที่ทำได้)
> hint: greedy — พยายามกระโดด 2 ก่อนเสมอ ถ้าช่อง +2 เป็น 1 ค่อยกระโดด 1

---
---

# 🔒 ZONE เฉลย (พยายามเองก่อน!)

<details>

## P1 — เต็ม (worked example I/O)
```go
package main
import ("bufio";"fmt";"os";"strconv";"strings")
func main() {
    sc := bufio.NewScanner(os.Stdin)
    sc.Buffer(make([]byte, 1<<20), 1<<20)
    read := func() string { sc.Scan(); return sc.Text() }
    t, _ := strconv.Atoi(read())
    for ; t > 0; t-- {
        _ = read()                       // n (ไม่ต้องใช้ก็ได้)
        sum := 0
        for _, f := range strings.Fields(read()) {
            x, _ := strconv.Atoi(f)
            if x%2 == 0 { sum += x }
        }
        fmt.Println(sum)
    }
}
```

## P2 — approach
map นับความถี่ · เดินทีละตัว `x`: ถ้ามี `k-x` ใน map แล้ว → บวก count[k-x] · แล้วค่อย count[x]++
(นับแบบ "จับคู่กับตัวก่อนหน้า" กัน double count) → O(n)

## P3 — approach
stack (`[]byte`) · map ปิด→เปิด `{')':'(', ']':'[', '}':'{'}`
เปิด → push · ปิด → ถ้า stack ว่าง หรือ top ≠ คู่ที่ต้องการ → NO · จบแล้ว stack ต้องว่าง → YES

## P4 — approach
```go
var dfs func(i, j int)
dfs = func(i, j int) {
    if i<0||i>=r||j<0||j>=c||g[i][j]==0 { return }
    g[i][j] = 0
    for di:=-1; di<=1; di++ { for dj:=-1; dj<=1; dj++ {
        if di!=0||dj!=0 { dfs(i+di, j+dj) }
    }}
}
// main: วนทุกช่อง เจอ 1 → count++ ; dfs(i,j)
```

## P5 — approach
```go
steps, i := 0, 0
for i < n-1 {
    if i+2 < n && c[i+2] == 0 { i += 2 } else { i += 1 }
    steps++
}
// print steps
```
</details>

---

## วิธีใช้ชุดนี้
1. ทำ P1 ก่อน (ฝึก parse หลาย test case ให้คล่อง) → นี่คือจุดที่พลาดรอบที่แล้ว
2. P2-P5 ทำเองให้ผ่าน sample ก่อนดูเฉลย
3. ติดตรงไหน แปะโค้ดมา เดี๋ยวช่วยไล่ bug ให้
4. ทำครบ 5 ข้อ = ครอบ pattern I/O + hash + stack + grid + greedy = ผ่าน OA ระดับต้น-กลางได้สบาย
