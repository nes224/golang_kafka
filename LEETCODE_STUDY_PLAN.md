# LeetCode Study Plan — เรียนตาม Pattern (สำหรับ OA/screening)

เป้า: สร้าง **reflex** เห็นโจทย์ → รู้ pattern → รู้ target O(n) ทันที
วิธีใช้: ทำ **ตามลำดับ pattern** (บนลงล่าง) · แต่ละข้อ **พูด complexity ออกมาก่อนเขียน** · ติ๊ก [x] เมื่อทำเองได้โดยไม่เปิดเฉลย

> 🟢 CORE (30 ข้อ — ทำให้ครบก่อน) · 🔵 EXTRA (เพิ่มถ้าเล็ง Agoda/Sea/LINE)
> ทุกข้อ: ทำได้ = เขียนใหม่จากศูนย์ผ่านได้ใน ~15 นาที (ไม่ใช่แค่ "เข้าใจ")

---

## 1. Arrays & Hashing (ฐานทุกอย่าง)
- [ ] 🟢 Two Sum — hash O(n)
- [ ] 🟢 Contains Duplicate — set
- [ ] 🟢 Valid Anagram — count map
- [ ] 🟢 Group Anagrams — hash key
- [ ] 🟢 Top K Frequent Elements — bucket/heap
- [ ] 🔵 Product of Array Except Self — prefix/suffix

## 2. Two Pointers
- [ ] 🟢 Valid Palindrome
- [ ] 🟢 Two Sum II (sorted) — เข้าใจ two-pointer ก่อนไป 3Sum
- [ ] 🟢 3Sum — sort + two-pointer + skip dup ⭐
- [ ] 🟢 Container With Most Water

## 3. Sliding Window
- [ ] 🟢 Best Time to Buy and Sell Stock
- [ ] 🟢 Longest Substring Without Repeating Characters ⭐
- [ ] 🔵 Longest Repeating Character Replacement

## 4. Stack
- [ ] 🟢 Valid Parentheses
- [ ] 🔵 Min Stack
- [ ] 🔵 Daily Temperatures — monotonic stack

## 5. Binary Search
- [ ] 🟢 Binary Search — template พื้นฐาน
- [ ] 🟢 Search in Rotated Sorted Array ⭐
- [ ] 🔵 Find Minimum in Rotated Sorted Array
- [ ] 🔵 Koko Eating Bananas — binary search on answer

## 6. Linked List
- [ ] 🟢 Reverse Linked List
- [ ] 🟢 Merge Two Sorted Lists
- [ ] 🟢 Linked List Cycle — fast/slow pointer
- [ ] 🔵 Remove Nth Node From End
- [ ] 🔵 Reorder List

## 7. Trees (recursion + BFS)
- [ ] 🟢 Invert Binary Tree
- [ ] 🟢 Maximum Depth of Binary Tree
- [ ] 🟢 Same Tree
- [ ] 🟢 Diameter of Binary Tree
- [ ] 🟢 Binary Tree Level Order Traversal — BFS ⭐
- [ ] 🟢 Validate Binary Search Tree ⭐
- [ ] 🔵 Lowest Common Ancestor of BST
- [ ] 🔵 Kth Smallest Element in a BST

## 8. Heap / Priority Queue
- [ ] 🟢 Kth Largest Element in an Array
- [ ] 🔵 Find Median from Data Stream (ยาก — เล็ก tier-1 ค่อยทำ)

## 9. Backtracking
- [ ] 🟢 Subsets
- [ ] 🟢 Combination Sum ⭐
- [ ] 🔵 Permutations

## 10. Graphs (BFS/DFS)
- [ ] 🟢 Number of Islands — grid DFS/BFS ⭐⭐ (ออกบ่อยสุด)
- [ ] 🟢 Clone Graph
- [ ] 🔵 Course Schedule — cycle detect / topological sort

## 11. Dynamic Programming (1D)
- [ ] 🟢 Climbing Stairs — DP เริ่มต้น
- [ ] 🟢 House Robber ⭐
- [ ] 🟢 Coin Change ⭐
- [ ] 🔵 Longest Increasing Subsequence
- [ ] 🔵 Word Break

## 12. Intervals
- [ ] 🟢 Merge Intervals ⭐
- [ ] 🟢 Insert Interval
- [ ] 🔵 Non-overlapping Intervals

## 13. Greedy
- [ ] 🟢 Maximum Subarray — Kadane ⭐
- [ ] 🟢 Jump Game

## 14. Bit Manipulation
- [ ] 🟢 Number of 1 Bits
- [ ] 🔵 Counting Bits
- [ ] 🔵 Missing Number

---

## แผนซ้อม (ปรับตามเวลาว่าง)

### เร่ง (2 สัปดาห์ · ~3 ข้อ/วัน) — เอาแค่ 🟢 CORE 30 ข้อ
| สัปดาห์ | pattern |
|---|---|
| 1 | Arrays/Hash · Two Pointer · Sliding Window · Stack · Binary Search |
| 2 | Linked List · Trees · Heap · Backtracking · Graph · DP · Interval · Greedy · Bit |

### เต็ม (4-6 สัปดาห์ · ~2 ข้อ/วัน) — 🟢 + 🔵 ครบ ~50 ข้อ
1 pattern/2-3 วัน · ทำ 🟢 ให้ผ่านก่อน แล้วค่อย 🔵 · จบ pattern → ทำ mixed review

---

## กติกาเหล็ก (ทำให้ pattern ติดจริง)
1. **พูด complexity ก่อนเขียนทุกข้อ** — "n=10^5 → ต้อง O(n) → hash" (สร้าง reflex ที่ใช้ในห้องสอบ)
2. **ติดเกิน 20 นาที → ดูเฉลย → ปิด → เขียนใหม่จากศูนย์** (นี่คือจุดที่ของขึ้น ไม่ใช่แค่อ่านเข้าใจ)
3. **ทำซ้ำข้อที่ mark** อีก 2-3 วันถัดมา (spaced repetition)
4. จบแต่ละ pattern → จด"trigger" 1 บรรทัด เช่น *"เห็น subarray/substring ยาวสุด → sliding window"*

## trigger cheat (จำ pattern จาก keyword ในโจทย์)
| เห็นคำนี้ในโจทย์ | นึกถึง |
|---|---|
| "คู่ที่รวมได้ X" / sorted | two-pointer หรือ hash |
| "subarray/substring ยาวสุด/สั้นสุด" | sliding window |
| "ทุก combination / ทุกวิธี" | backtracking |
| "grid ติดกัน / เกาะ / เชื่อม" | BFS/DFS |
| "จำนวนวิธี / น้อยสุด / มากสุด (ทีละสเต็ป)" | DP |
| "k ตัวที่มาก/บ่อยสุด" | heap |
| "ช่วงเวลา / interval ทับกัน" | sort + merge |

> เริ่มเลย: **Arrays/Hashing → Two Sum** วันนี้ · ทำตามลำดับ อย่าข้าม pattern
