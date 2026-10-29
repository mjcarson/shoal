# A perf report's symbols of one set of threads, sorted into what they do, as shares of its samples
# (O104's diagnosis, docs/src/object-storage/store-optimizations/object-lane.md). The report is
# `perf report --no-children --comms <threads> --percentage relative --sort dso,symbol --stdio
# --percent-limit 0` written to a file: `python3 -I stores-ab-perf.py <report.txt>`. libc's memmove and
# memset are named by their addresses in europa's libc, which has no symbols for them: another
# host's libc needs its own ranges, read off `objdump -d` around the addresses it samples
import collections, re, sys

RULES = [
    ("kTLS encryption (AES-GCM and its walk)", r"aes_gcm|gcm_|skcipher|kernel_fpu|tls_|scatterwalk|ghash|crypto_|aead"),
    ("kernel page zeroing and allocation", r"kernel_init_pages|clear_page|__pi_memset|alloc_pages|free_pages|__free_frozen|folio|rmqueue|get_page_from_freelist|_raw_spin_lock_irqsave|free_unref|zone_|pcp|__alloc_frozen|post_alloc_hook|prep_new_page|do_anonymous_page|handle_mm_fault|__handle_mm_fault|do_user_addr_fault|exc_page_fault|asm_exc_page_fault|lru_|mmap|munmap|vma|unmap|zap_|tlb|page_fault|mm_|pte|pmd|__mod_lruvec|lruvec"),
    ("memory cgroup accounting", r"memcg|mem_cgroup|refill_stock|obj_cgroup|charge"),
    ("kernel copies", r"_copy_to_iter|_copy_from_iter|copy_user|rep_movs|copyin|copyout|memcpy_orig|__memcpy|skb_copy_datagram|simple_copy"),
    ("TCP/IP over loopback", r"tcp|ip_|__ip|ipv4|skb|net_|netif|nf_|sk_|__sk|sock|loopback|process_backlog|dev_|inet|__dev|napi|neigh|xmit|__netif|rcv|recv|sendmsg|__sys_send|__sys_recv|validate_xmit|__kfree_skb|kfree_skb|tso|gso|csum"),
    ("syscall entry and mitigations", r"entry_|srso|syscall|do_syscall|__x64_sys|retbleed|_ret$|return_thunk|clear_bhb|switch_|x86_"),
    ("io_uring and the DMA reads", r"io_|uring|xfs|iomap|blk|bio|nvme|dm_|loop_|iov_iter|submit|__io|filemap|xas_|direct"),
    ("scheduling and timers", r"schedule|__schedule|timer|hrtimer|tick|sched|enqueue|dequeue|wake|update_|ktime|read_tsc|clock|cpuidle|idle|rcu|irq|softirq|__do_softirq|smp_call|ipi|futex|ttwu|select_task|pick_"),
    ("slab allocation", r"kmem_cache|kmalloc|kfree|slab|__slab|alloc_skb|ksize|__ksize|krealloc"),
]
USER = [
    ("memmove in libc (copies)", lambda dso, sym: dso == "libc.so.6" and 0x1b0b00 <= int(sym, 16) < 0x1b1200 if sym.startswith("0x") else False),
    ("memset in libc (zeroing buffers)", lambda dso, sym: dso == "libc.so.6" and 0x1b1200 <= int(sym, 16) < 0x1b1500 if sym.startswith("0x") else False),
    ("CRC-64 of units (crc_fast)", lambda dso, sym: "crc_fast" in sym),
    ("glommio", lambda dso, sym: "glommio" in sym),
]
totals = collections.Counter()
other = collections.Counter()
for line in open(sys.argv[1]):
    m = re.match(r"\s*([\d.]+)% (\S+) \[(.)\] (.*?) - -", line)
    if not m:
        continue
    share, dso, kind, sym = float(m.group(1)), m.group(2), m.group(3), m.group(4).strip()
    placed = None
    if kind == "k":
        for name, pattern in RULES:
            if re.search(pattern, sym):
                placed = name
                break
        placed = placed or "kernel, other"
    else:
        for name, test in USER:
            try:
                if test(dso, sym):
                    placed = name
                    break
            except ValueError:
                pass
        placed = placed or f"user, other ({dso})"
    totals[placed] += share
    if placed.endswith("other") or placed.startswith("user, other"):
        other[(placed, sym)] += share
for name, share in totals.most_common():
    print(f"{share:6.2f}%  {name}")
print("-- the largest of the others")
for (name, sym), share in other.most_common(12):
    print(f"{share:6.2f}%  {name}: {sym[:90]}")
