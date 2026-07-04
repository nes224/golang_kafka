# LeetCode — แผนติวคืนเดียว + Template (บริบทไทย)

เป้า: ผ่านรอบ screening / OA เข้าไปสัมภาษณ์ · เน้นลึกไม่เน้นกว้าง · ทวนไฟล์เดียวจบ

---

## 0. มาตรฐานสัมภาษณ์โค้ดในไทย (รู้ไว้จะได้เล็งถูก)

| กลุ่มบริษัท | ระดับที่เจอ | ตัวอย่าง |
|---|---|---|
| **Software house / SME / agency** | เบา — array/string ง่ายๆ, FizzBuzz, take-home, บาง OA HackerRank easy | ทั่วไป |
| **Product / Fintech ไทย** | LeetCode **easy–medium** (OA + สัมสด) | KBTG, SCB 10x/Tech, True Digital, Ascend, LMWN (LINE MAN Wongnai) |
| **Global/Tier-1 in TH** | LeetCode **medium–hard** จริงจัง · algo screening | **Agoda** (ขึ้นชื่อเรื่อง algo), Sea/Shopee/Garena, LINE, Grab, Booking, Google BKK |

**สรุป:** ถ้าไม่ได้เล็ง Agoda/Sea/Google — **Blind 75 subset (easy–medium) พอสำหรับ 80% ของงานในไทย** · ส่วนใหญ่ screening = HackerRank/Codility OA 1-2 ข้อ medium ภายใน 60-90 นาที
**ภาษา:** เขียนภาษาไหนก็ได้ที่ถนัด (Go ได้) · แต่ **Python เร็วสุดบน LeetCode/OA** — แนะนำใช้ตอนสอบ

---

## 1. Checklist คืนนี้ (~23 ข้อ · เช็คทีละข้อ)

### 🔴 Tier 1 — 17 ข้อที่มี (ทวนให้ลื่น · ข้อไหนติด mark ไว้)
- [ ] Two Sum
- [ ] Best Time to Buy and Sell Stock
- [ ] Contains Duplicate
- [ ] Search in Rotated Sorted Array
- [ ] Container With Most Water
- [ ] Number of 1 Bits
- [ ] Climbing Stairs
- [ ] Coin Change
- [ ] Clone Graph
- [ ] Insert Interval
- [ ] Merge Intervals
- [ ] Reverse a Linked List
- [ ] Longest Substring Without Repeating Characters
- [ ] Valid Parentheses
- [ ] Valid Palindrome
- [ ] Maximum Depth of Binary Tree
- [ ] Same Tree

### 🟠 Tier 2 — เติม 6 ข้อ ROI สูงสุด (ของใหม่จริง)
- [ ] **3 Sum** — two-pointer + sort ⭐
- [ ] **Number of Islands** — matrix BFS/DFS ⭐ (ออกบ่อยสุด)
- [ ] **Merge Two Sorted Lists** — linked list ง่าย เจอชัวร์
- [ ] **Binary Tree Level Order Traversal** — BFS tree
- [ ] **Validate Binary Search Tree** — DFS + bound
- [ ] **Valid Anagram** — hash · ฟรีแต้ม 3 นาที

### 🟢 Tier 3 — เฉพาะถ้าเวลาเหลือ
- [ ] Product of Array Except Self
- [ ] Maximum Subarray (Kadane)
- [ ] Detect Cycle in a Linked List
- [ ] Invert Binary Tree
- [ ] House Robber
- [ ] Top K Frequent Elements (heap)

### ⛔️ ข้ามคืนนี้ (ยาก/ออกน้อยในรอบ screening)
Merge K Sorted Lists · Trie (Implement/Word Search II) · Serialize Tree · Binary Tree Max Path Sum · Median from Data Stream · Minimum Window Substring · LCS · Decode Ways · Premium ทุกข้อ

---

## 2. ลำดับทำคืนนี้ (timebox ~4-5 ชม.)
1. **ชม.1-2:** ไล่ Tier 1 (ข้อละ ~7 นาที) — ลื่นข้าม / ติด mark
2. **ชม.3:** ทำ Tier 2 ทั้ง 6
3. **ชม.4:** ทวนข้อ mark + ท่อง template ข้างล่างจนเห็นโจทย์นึกออก
4. **นอน 5-6 ชม.** (สำคัญกว่าได้เพิ่ม 2 ข้อ)

---

## 3. Template (ท่องให้ขึ้นใจ — เห็นโจทย์ปุ๊บเขียนโครงปั๊บ)

### Two Pointer (sorted array / palindrome)
```python
l, r = 0, len(a) - 1
while l < r:
    if cond: l += 1
    elif other: r -= 1
    else: ...  # เจอคำตอบ
```

### Sliding Window (substring/subarray ที่ยาวสุด/สั้นสุด)
```python
seen = {}          # หรือ set / count
l = 0; best = 0
for r in range(len(s)):
    # ขยายขวา: อัปเดต state ด้วย s[r]
    while invalid:        # หดซ้ายจนกลับมา valid
        # เอา s[l] ออกจาก state
        l += 1
    best = max(best, r - l + 1)
```

### BFS (matrix / tree level order / shortest path)
```python
from collections import deque
q = deque([start]); seen = {start}
while q:
    node = q.popleft()
    for nxt in neighbors(node):
        if nxt not in seen:
            seen.add(nxt); q.append(nxt)
# tree level order: วน for _ in range(len(q)) ต่อชั้น
```

### DFS (grid islands / tree / backtracking)
```python
def dfs(r, c):
    if out_of_bound or grid[r][c] != '1': return
    grid[r][c] = '0'                      # mark visited
    for dr, dc in ((1,0),(-1,0),(0,1),(0,-1)):
        dfs(r+dr, c+dc)
```

### Binary Search
```python
lo, hi = 0, len(a) - 1
while lo <= hi:
    mid = (lo + hi) // 2
    if a[mid] == target: return mid
    if a[mid] < target: lo = mid + 1
    else: hi = mid - 1
return -1
```

### DP 1 มิติ (climbing stairs / house robber)
```python
dp = [0] * (n + 1)
dp[0], dp[1] = base0, base1
for i in range(2, n + 1):
    dp[i] = f(dp[i-1], dp[i-2])   # เช่น max(dp[i-1], dp[i-2]+val[i])
return dp[n]
```

### Hash count (anagram / two sum / top-k)
```python
from collections import Counter, defaultdict
cnt = Counter(s)          # นับความถี่
seen = {}                 # two sum: seen[target-x] -> index
```

### Linked List (reverse / merge)
```python
# reverse
prev = None
while head:
    head.next, prev, head = prev, head, head.next
return prev
# dummy สำหรับ merge/insert
dummy = ListNode(); tail = dummy
```

---

## 4. กันพลาดในห้องสัม (สำคัญพอๆ กับทำได้)
1. **พูดวิธีคิดออกมา** — brute force ก่อนก็ได้ เขาให้คะแนน process มากกว่าคำตอบเป๊ะ
2. **ถาม clarify** ก่อนเขียน (input range? ว่างได้ไหม? มี dup ไหม?)
3. เขียนเสร็จ **ไล่ test เคสมือ** + บอก **Time/Space O(n)**
4. ติดจริง → ขอ hint ดีกว่าเงียบ

---

## 5. ลิงก์คุ้มเปิด (15 นาที)
- **14 Patterns to Ace Any Coding Interview** (เห็นภาพ pattern รวม)
- **Grind 75** (ถ้าอยากได้ลิสต์เรียง priority ต่อจากนี้)

> จำไว้: **23 ข้อเข้าใจลึก (ทำซ้ำจนอธิบายได้) > 75 ข้อแบบจำ** · คืนนี้เอาแค่นี้ พอ 💪
