// "Put these in order" component: click steps in the right sequence; instant feedback per click.
// Markup:
//   <div class="order" data-steps='["First","Second","Third"]'></div>
// Steps are given in the correct order; the component shuffles them for display.
document.querySelectorAll(".order").forEach((el) => {
  const steps = JSON.parse(el.dataset.steps);
  let next = 0, mistakes = 0;
  const picked = document.createElement("ol");
  picked.className = "picked";
  const fb = document.createElement("p");
  fb.className = "feedback";
  const pool = steps.map((s, i) => ({ s, i })).sort(() => Math.random() - 0.5);

  pool.forEach(({ s, i }) => {
    const b = document.createElement("button");
    b.textContent = s;
    b.addEventListener("click", () => {
      if (i === next) {
        b.disabled = true; b.classList.add("right"); b.classList.remove("wrong");
        const li = document.createElement("li"); li.textContent = s; picked.append(li);
        next++;
        fb.className = "feedback right";
        fb.textContent = next === steps.length
          ? `Done${mistakes ? ` with ${mistakes} wrong pick${mistakes > 1 ? "s" : ""}` : ", no mistakes"}. Try it again tomorrow without looking.`
          : "Yes. What comes next?";
      } else {
        mistakes++; b.classList.add("wrong");
        fb.className = "feedback wrong";
        fb.textContent = "Not next. Think about where the data is right now.";
        setTimeout(() => b.classList.remove("wrong"), 700);
      }
    });
    el.append(b);
  });
  el.append(picked, fb);
});
