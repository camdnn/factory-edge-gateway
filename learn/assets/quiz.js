// Multiple-choice quiz component with instant feedback.
// Markup:
//   <div class="quiz">
//     <div class="item" data-answer="2" data-why="Explanation shown after answering.">
//       <p class="q">Question?</p>
//       <button>Option 0</button><button>Option 1</button><button>Option 2</button>
//     </div>
//   </div>
// data-answer is the 0-based index of the correct button.
document.querySelectorAll(".quiz").forEach((quiz) => {
  const items = [...quiz.querySelectorAll(".item")];
  let answered = 0, correct = 0;
  const score = document.createElement("p");
  score.className = "score";
  quiz.append(score);

  items.forEach((item) => {
    const buttons = [...item.querySelectorAll("button")];
    const fb = document.createElement("p");
    fb.className = "feedback";
    item.append(fb);
    buttons.forEach((b, i) => b.addEventListener("click", () => {
      const right = i === Number(item.dataset.answer);
      buttons.forEach((x) => (x.disabled = true));
      b.classList.add(right ? "right" : "wrong");
      buttons[Number(item.dataset.answer)].classList.add("right");
      fb.className = "feedback " + (right ? "right" : "wrong");
      fb.textContent = (right ? "Correct. " : "Not quite. ") + (item.dataset.why || "");
      answered++; if (right) correct++;
      if (answered === items.length) score.textContent = `You got ${correct} of ${items.length}.`;
    }));
  });
});
