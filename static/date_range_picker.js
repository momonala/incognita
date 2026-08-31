/* Single-calendar date range picker.
 *
 * Click semantics (per opening of the panel):
 *   1st click  -> anchors a new selection on that day
 *   2nd click  -> closes the range; order-agnostic, so either bound may be picked first
 *                 (same day twice yields a one-day range)
 *   3rd+ click -> nudges whichever bound is nearest to the clicked day
 */
(function (global) {
    "use strict";

    const DAY_MS = 86400000;
    const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ];

    const toUTC = (iso) => new Date(iso + "T00:00:00Z");
    const toISO = (date) => date.toISOString().slice(0, 10);
    const utcDay = (y, m, d) => new Date(Date.UTC(y, m, d));
    const addDays = (iso, n) => toISO(new Date(toUTC(iso).getTime() + n * DAY_MS));
    const daysBetween = (a, b) => Math.abs(toUTC(a) - toUTC(b)) / DAY_MS;

    function DateRangePicker(root, options) {
        const opts = options || {};
        const trigger = root.querySelector("[data-drp-trigger]");
        const label = root.querySelector("[data-drp-label]");
        const panel = root.querySelector("[data-drp-panel]");

        let start = opts.start;
        let end = opts.end;
        let phase = "fresh"; // fresh | pending | complete
        let anchor = null;
        let hover = null;
        let cursor = toUTC(start); // month currently on screen
        let open = false;
        let committed = start + "/" + end;

        // --- panel scaffolding -------------------------------------------------
        panel.innerHTML =
            '<div class="drp__header">' +
            '<button type="button" class="drp__step" data-drp-prev aria-label="Previous month">&#8249;</button>' +
            '<div class="drp__period">' +
            '<select class="drp__select" data-drp-month aria-label="Month"></select>' +
            '<select class="drp__select" data-drp-year aria-label="Year"></select>' +
            "</div>" +
            '<button type="button" class="drp__step" data-drp-next aria-label="Next month">&#8250;</button>' +
            "</div>" +
            '<div class="drp__weekdays">' +
            WEEKDAYS.map((d) => '<span class="drp__weekday">' + d + "</span>").join("") +
            "</div>" +
            '<div class="drp__grid" data-drp-grid></div>' +
            '<div class="drp__footer">' +
            '<div class="drp__bound">' +
            '<span class="drp__bound-label">From</span>' +
            '<span class="drp__bound-date" data-drp-from></span>' +
            "</div>" +
            '<span class="drp__bound-arrow" aria-hidden="true">&#8594;</span>' +
            '<div class="drp__bound">' +
            '<span class="drp__bound-label">To</span>' +
            '<span class="drp__bound-date" data-drp-to></span>' +
            "</div>" +
            '<div class="drp__bound drp__bound--count">' +
            '<span class="drp__bound-label">Days</span>' +
            '<span class="drp__bound-date" data-drp-summary></span>' +
            "</div>" +
            "</div>";

        const monthSelect = panel.querySelector("[data-drp-month]");
        const yearSelect = panel.querySelector("[data-drp-year]");
        const grid = panel.querySelector("[data-drp-grid]");
        const summary = panel.querySelector("[data-drp-summary]");
        const fromText = panel.querySelector("[data-drp-from]");
        const toText = panel.querySelector("[data-drp-to]");
        let cells = []; // { el, iso, inMonth } for the month on screen

        monthSelect.innerHTML = MONTHS.map((m, i) => '<option value="' + i + '">' + m + "</option>").join("");
        const thisYear = new Date().getUTCFullYear();
        const firstYear = Math.min(thisYear - 20, toUTC(start).getUTCFullYear());
        const lastYear = Math.max(thisYear + 1, toUTC(end).getUTCFullYear());
        let years = "";
        for (let y = firstYear; y <= lastYear; y++) years += '<option value="' + y + '">' + y + "</option>";
        yearSelect.innerHTML = years;

        // --- rendering ---------------------------------------------------------
        function tentative() {
            // The range to paint: either the settled one, or the live one being dragged out.
            if (phase === "pending" && anchor) {
                const other = hover || anchor;
                return anchor <= other ? [anchor, other] : [other, anchor];
            }
            return [start, end];
        }

        function renderLabel() {
            label.textContent = start === end ? start : start + "  →  " + end;
        }

        // "2026-08-14" -> "14 Aug 2026" — easier to read at a glance than ISO.
        function pretty(iso) {
            const date = toUTC(iso);
            return date.getUTCDate() + " " + MONTHS[date.getUTCMonth()].slice(0, 3) + " " + date.getUTCFullYear();
        }

        function renderSummary() {
            const [lo, hi] = tentative();
            const n = daysBetween(lo, hi) + 1;
            fromText.textContent = pretty(lo);
            toText.textContent = pretty(hi);
            summary.textContent = String(n);
        }

        // Repaints selection state on the existing cells. Never touches the DOM
        // structure — rebuilding mid-hover would swallow the next click, since a
        // button replaced between mousedown and mouseup never fires one.
        function paint() {
            const [lo, hi] = tentative();
            const today = toISO(new Date());
            for (const cell of cells) {
                const { el, iso } = cell;
                el.classList.toggle("drp__day--muted", !cell.inMonth);
                el.classList.toggle("drp__day--today", iso === today);
                el.classList.toggle("drp__day--in-range", iso >= lo && iso <= hi);
                el.classList.toggle("drp__day--start", iso === lo);
                el.classList.toggle("drp__day--end", iso === hi);
            }
            renderSummary();
        }

        function render() {
            const year = cursor.getUTCFullYear();
            const month = cursor.getUTCMonth();
            monthSelect.value = String(month);
            yearSelect.value = String(year);

            const first = utcDay(year, month, 1);
            const lead = (first.getUTCDay() + 6) % 7; // Monday-first grid
            const daysInMonth = utcDay(year, month + 1, 0).getUTCDate();
            const cellCount = Math.ceil((lead + daysInMonth) / 7) * 7;

            const fragment = document.createDocumentFragment();
            cells = [];
            for (let i = 0; i < cellCount; i++) {
                const date = utcDay(year, month, i - lead + 1);
                const iso = toISO(date);
                const el = document.createElement("button");
                el.type = "button";
                el.className = "drp__day";
                el.dataset.date = iso;
                el.textContent = String(date.getUTCDate());
                fragment.appendChild(el);
                cells.push({ el, iso, inMonth: date.getUTCMonth() === month });
            }
            grid.replaceChildren(fragment);
            paint();
        }

        // --- selection ---------------------------------------------------------
        function pick(iso) {
            if (phase === "fresh") {
                anchor = iso;
                start = iso;
                end = iso;
                phase = "pending";
            } else if (phase === "pending") {
                start = anchor <= iso ? anchor : iso;
                end = anchor <= iso ? iso : anchor;
                anchor = null;
                phase = "complete";
            } else {
                // Move the nearer bound, whether the click lands inside or outside the range.
                if (iso < start) start = iso;
                else if (iso > end) end = iso;
                else if (daysBetween(iso, start) <= daysBetween(iso, end)) start = iso;
                else end = iso;
            }
            hover = null;
            renderLabel();
            paint();
            if (opts.onChange) opts.onChange(start, end);
        }

        function setOpen(next) {
            if (open === next) return;
            open = next;
            panel.hidden = !open;
            trigger.setAttribute("aria-expanded", String(open));
            root.classList.toggle("drp--open", open);
            if (open) {
                phase = "fresh";
                anchor = null;
                hover = null;
                cursor = toUTC(start);
                render();
                position();
            } else if (opts.onClose) {
                const now = start + "/" + end;
                const changed = now !== committed;
                committed = now;
                opts.onClose(changed);
            }
        }

        // The page shell clips overflow, so the panel is fixed and placed by hand.
        function position() {
            const rect = trigger.getBoundingClientRect();
            panel.style.top = rect.bottom + 8 + "px";
            const width = panel.offsetWidth;
            const left = Math.min(rect.left, window.innerWidth - width - 12);
            panel.style.left = Math.max(12, left) + "px";
        }

        // --- events ------------------------------------------------------------
        trigger.addEventListener("click", () => setOpen(!open));

        panel.querySelector("[data-drp-prev]").addEventListener("click", () => {
            cursor = utcDay(cursor.getUTCFullYear(), cursor.getUTCMonth() - 1, 1);
            render();
        });
        panel.querySelector("[data-drp-next]").addEventListener("click", () => {
            cursor = utcDay(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1);
            render();
        });
        monthSelect.addEventListener("change", () => {
            cursor = utcDay(cursor.getUTCFullYear(), Number(monthSelect.value), 1);
            render();
        });
        yearSelect.addEventListener("change", () => {
            cursor = utcDay(Number(yearSelect.value), cursor.getUTCMonth(), 1);
            render();
        });

        grid.addEventListener("click", (event) => {
            const cell = event.target.closest("[data-date]");
            if (cell) pick(cell.dataset.date);
        });
        grid.addEventListener("mouseover", (event) => {
            const cell = event.target.closest("[data-date]");
            if (!cell || phase !== "pending" || hover === cell.dataset.date) return;
            hover = cell.dataset.date;
            paint();
        });
        grid.addEventListener("mouseleave", () => {
            if (phase !== "pending" || hover === null) return;
            hover = null;
            paint();
        });

        document.addEventListener("mousedown", (event) => {
            if (open && !root.contains(event.target)) setOpen(false);
        });
        window.addEventListener("resize", () => { if (open) position(); });
        document.addEventListener("keydown", (event) => {
            if (event.key === "Escape") setOpen(false);
        });

        renderLabel();
        render();

        return {
            get start() { return start; },
            get end() { return end; },
            close: () => setOpen(false),
            set(nextStart, nextEnd) {
                start = nextStart;
                end = nextEnd;
                committed = start + "/" + end;
                phase = "fresh";
                anchor = null;
                cursor = toUTC(start);
                renderLabel();
                render();
            },
            shift(delta) {
                this.set(addDays(start, delta), addDays(end, delta));
            },
        };
    }

    global.DateRangePicker = DateRangePicker;
})(window);
