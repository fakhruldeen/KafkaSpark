#!/usr/bin/env python3
"""
Enterprise Interactive Knowledge Quiz Engine
Kafka & Spark Enterprise Training Curriculum
Author: Mohamed Fouad Fakhruldeen (Fakhr.me)
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path

# --- ANSI Color Palette ---
CYAN = "\033[96m"
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
RESET = "\033[0m"

BASE_DIR = Path(__file__).resolve().parent
QUIZ_DATA_DIR = BASE_DIR / "quizzes"
STATE_DIR = BASE_DIR / ".quiz_states"

def banner(title: str, subtitle: str = ""):
    print(f"\n{CYAN}{BOLD}======================================================================{RESET}")
    print(f" {YELLOW}{BOLD}{title}{RESET}")
    if subtitle:
        print(f" {CYAN}{subtitle}{RESET}")
    print(f"{CYAN}{BOLD}======================================================================{RESET}\n")

def get_state_file(session_id: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{session_id.lower()}_state.json"

def load_state(session_id: str) -> dict:
    state_file = get_state_file(session_id)
    if state_file.exists():
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "session": session_id.lower(),
        "attempts": [],
        "highest_score": 0,
        "max_score": 10,
        "completed": False
    }

def save_state(session_id: str, state: dict):
    state_file = get_state_file(session_id)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def load_quiz_data(session_id: str) -> dict:
    json_path = QUIZ_DATA_DIR / f"{session_id.lower()}_quiz.json"
    if not json_path.exists():
        print(f"{RED}Error: Quiz file not found for session '{session_id}' at: {json_path}{RESET}")
        sys.exit(1)
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def clean_input(raw: str) -> list:
    """Parses user input like 'A, C' or 'A C' or 'b' into sorted uppercase list ['A', 'C']"""
    tokens = raw.replace(",", " ").replace(";", " ").upper().split()
    return sorted(list(set(tokens)))

def display_question(index: int, total: int, q: dict) -> list:
    q_type = q.get("type", "single")
    is_multi = (q_type == "multiple")
    type_badge = f"{YELLOW}[Multiple Choice — Select ALL that apply (e.g. A, C)]{RESET}" if is_multi else f"{BLUE}[Single Choice — Select 1 option (e.g. B)]{RESET}"

    print(f"{BOLD}{CYAN}----------------------------------------------------------------------{RESET}")
    print(f"{BOLD}Question {index} of {total}:{RESET} {type_badge}")
    print(f"{BOLD}{q['question']}{RESET}\n")

    for opt_key in sorted(q["options"].keys()):
        print(f"  {BOLD}{opt_key}){RESET} {q['options'][opt_key]}")
    print()

    valid_keys = set(q["options"].keys())
    while True:
        try:
            prompt = f"{GREEN}Your choice{'s' if is_multi else ''} > {RESET}"
            user_input = input(prompt).strip()
            if not user_input:
                print(f"{YELLOW}Please enter an option letter.{RESET}")
                continue
            parsed = clean_input(user_input)
            
            # Validate input letters
            invalid = [ch for ch in parsed if ch not in valid_keys]
            if invalid:
                print(f"{RED}Invalid option(s): {', '.join(invalid)}. Choose only from ({', '.join(sorted(valid_keys))}).{RESET}")
                continue
            
            if not is_multi and len(parsed) > 1:
                print(f"{RED}This is a single choice question. Enter only 1 letter.{RESET}")
                continue
            
            return parsed
        except (KeyboardInterrupt, EOFError):
            print(f"\n{RED}Quiz aborted by user.{RESET}")
            sys.exit(0)

def render_explanations(quiz: dict, questions_to_explain: list, show_correct_answer: bool = True):
    for q_idx in questions_to_explain:
        q = quiz["questions"][q_idx]
        print(f"\n{CYAN}{BOLD}======================================================================{RESET}")
        print(f"{YELLOW}{BOLD}Question {q_idx + 1}: {q['question']}{RESET}")
        print(f"{CYAN}----------------------------------------------------------------------{RESET}")
        
        if show_correct_answer:
            correct_str = ", ".join(q["correct"])
            print(f"★ {GREEN}{BOLD}Correct Answer:{RESET} {GREEN}{BOLD}{correct_str}{RESET}")
        
        print(f"\n{BOLD}Architectural Breakdown & Explanation:{RESET}")
        print(f"{q['explanation']}\n")
        
        # Option by option breakdown
        if "option_explanations" in q:
            print(f"{UNDERLINE}Why other options were incorrect / Analysis of Choices:{RESET}")
            for opt, exp in q["option_explanations"].items():
                is_correct = opt in q["correct"]
                mark = f"{GREEN}✓" if is_correct else f"{RED}✗"
                print(f"  {mark} {BOLD}Option {opt}:{RESET} {exp}")
    print(f"{CYAN}{BOLD}======================================================================{RESET}\n")

def run_quiz(session_id: str, force_reset: bool = False):
    quiz = load_quiz_data(session_id)
    state = load_state(session_id)

    if force_reset:
        state = {
            "session": session_id.lower(),
            "attempts": [],
            "highest_score": 0,
            "max_score": len(quiz["questions"]),
            "completed": False
        }
        save_state(session_id, state)
        print(f"{YELLOW}State has been reset. You have 3 fresh attempts for session '{session_id}'.{RESET}\n")
        return

    current_attempts = len(state["attempts"])

    # If student already completed all 3 attempts
    if current_attempts >= 3 or state.get("completed", False):
        banner(f"🎯 {quiz['title']}", f"Official Assessment Results — Completed")
        print(f"{YELLOW}You have already completed all 3 permitted attempts for this session.{RESET}\n")
        print(f"{BOLD}Attempt History:{RESET}")
        for att in state["attempts"]:
            print(f"  • Attempt {att['attempt_number']}: {BOLD}{att['score']} / {att['max_score']}{RESET} ({att['score']/att['max_score']*100:.0f}%) — [{att['timestamp']}]")
        
        highest = state.get("highest_score", 0)
        max_s = state.get("max_score", len(quiz["questions"]))
        pct = (highest / max_s) * 100
        print(f"\n{GREEN}{BOLD}★ FINAL OFFICIAL CERTIFIED SCORE (Highest Attempt): {highest} / {max_s} ({pct:.0f}%) ★{RESET}\n")

        print(f"{CYAN}Reviewing Full Master Solution Guide for all 10 questions:{RESET}")
        render_explanations(quiz, list(range(len(quiz["questions"]))), show_correct_answer=True)
        return

    # Start new attempt
    attempt_num = current_attempts + 1
    banner(
        f"🎯 {quiz['title']}",
        f"Pre-Lab Knowledge Gate — Attempt [{attempt_num} of 3]"
    )
    print(f"📌 {BOLD}Instructions:{RESET}")
    print(f"   • Total Questions: {len(quiz['questions'])} (Single & Multiple Choice)")
    print(f"   • Attempts Allowed: 3")
    print(f"   • Attempt 1 & 2: Diagnostic feedback explaining wrong choices at the end.")
    print(f"   • Attempt 3 (or 100% score): Official highest score locked + Full solution guide to all questions.")
    print(f"{CYAN}----------------------------------------------------------------------{RESET}\n")

    if sys.stdin.isatty():
        input(f"{GREEN}Press ENTER to start Attempt {attempt_num}... {RESET}")
    print()

    user_answers = {}
    wrong_indices = []
    correct_count = 0

    for i, q in enumerate(quiz["questions"]):
        ans = display_question(i + 1, len(quiz["questions"]), q)
        user_answers[str(i)] = ans
        
        # Check correctness (sets match)
        if set(ans) == set(q["correct"]):
            correct_count += 1
        else:
            wrong_indices.append(i)

    score = correct_count
    total = len(quiz["questions"])
    pct = (score / total) * 100

    # Record attempt
    attempt_record = {
        "attempt_number": attempt_num,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "score": score,
        "max_score": total,
        "user_answers": user_answers,
        "wrong_questions": wrong_indices
    }
    state["attempts"].append(attempt_record)
    if score > state.get("highest_score", 0):
        state["highest_score"] = score
    state["max_score"] = total

    if attempt_num >= 3 or score == total:
        state["completed"] = True

    save_state(session_id, state)

    # --- Result Presentation ---
    print(f"\n{CYAN}{BOLD}======================================================================{RESET}")
    print(f" {YELLOW}{BOLD}📊 ATTEMPT {attempt_num} OF 3 SUMMARY RESULT{RESET}")
    print(f"{CYAN}{BOLD}======================================================================{RESET}")
    color = GREEN if pct >= 80 else (YELLOW if pct >= 50 else RED)
    print(f" Your Score on this Attempt: {color}{BOLD}{score} / {total} ({pct:.1f}%){RESET}")
    print(f" Current Highest Score:     {GREEN}{BOLD}{state['highest_score']} / {total}{RESET}")
    print(f"{CYAN}----------------------------------------------------------------------{RESET}")

    # Case A: Attempt 1 or 2 (Not perfect score)
    if attempt_num < 3 and score < total:
        remaining = 3 - attempt_num
        print(f"\n{MAGENTA}{BOLD}🔍 DIAGNOSTIC FEEDBACK ON INCORRECT ANSWERS ({len(wrong_indices)} questions):{RESET}")
        print(f"{YELLOW}Review why your selections were wrong before taking Attempt {attempt_num + 1}:{RESET}\n")
        
        # In first 2 attempts, explain why their chosen options were wrong without giving away the exact answer
        for q_idx in wrong_indices:
            q = quiz["questions"][q_idx]
            user_choice = ", ".join(user_answers[str(q_idx)])
            print(f"{RED}✗ Question {q_idx + 1}:{RESET} {BOLD}{q['question']}{RESET}")
            print(f"  Your Selection: {RED}{BOLD}{user_choice}{RESET}")
            print(f"  {BOLD}Diagnostic Analysis of Why This is Incorrect:{RESET}")
            
            # Show specific misconception analysis
            for chosen in user_answers[str(q_idx)]:
                if chosen not in q["correct"] and "option_explanations" in q and chosen in q["option_explanations"]:
                    print(f"   • {RED}Why ({chosen}) is wrong:{RESET} {q['option_explanations'][chosen]}")
            
            if "diagnostic_hint" in q:
                print(f"   💡 {CYAN}{BOLD}Concept Hint for Next Attempt:{RESET} {q['diagnostic_hint']}")
            print(f"{CYAN}----------------------------------------------------------------------{RESET}")

        print(f"\n{YELLOW}{BOLD}🔄 You have {remaining} attempt(s) remaining!{RESET}")
        print(f"Whenever you are ready to retry, execute:")
        print(f"   {GREEN}python3 quiz.py {session_id}{RESET}\n")

    # Case B: Attempt 3 reached OR Perfect Score achieved!
    else:
        if score == total:
            print(f"\n{GREEN}{BOLD}🎉 PERFECT SCORE! 100% MASTERY ACHIEVED ON ATTEMPT {attempt_num}! 🎉{RESET}\n")
        else:
            print(f"\n{YELLOW}{BOLD}★ All 3 Attempts Completed! ★{RESET}")
            print(f"Your official locked score is the HIGHEST of all 3 attempts: {GREEN}{BOLD}{state['highest_score']} / {total} ({state['highest_score']/total*100:.0f}%){RESET}\n")

        print(f"{CYAN}{BOLD}======================================================================{RESET}")
        print(f" {YELLOW}{BOLD}📖 FULL ARCHITECTURAL MASTER SOLUTION GUIDE (ALL 10 QUESTIONS){RESET}")
        print(f"{CYAN}{BOLD}======================================================================{RESET}")
        render_explanations(quiz, list(range(total)), show_correct_answer=True)
        print(f"\n{GREEN}{BOLD}✓ You are now 100% prepared to begin the Hands-On Lab! Proceed to the terminal.{RESET}\n")

def main():
    parser = argparse.ArgumentParser(description="Enterprise Kafka & Spark Interactive Knowledge Quiz Engine")
    parser.add_argument("session", nargs="?", default="s01", help="Session ID to quiz on (e.g. s01, s02, s03)")
    parser.add_argument("--reset", action="store_true", help="Reset quiz attempt history for this session (Instructor override)")
    parser.add_argument("--list", action="store_true", help="List available session quizzes")
    args = parser.parse_args()

    if args.list:
        print(f"\n{CYAN}{BOLD}Available Knowledge Quizzes:{RESET}")
        if QUIZ_DATA_DIR.exists():
            for f in sorted(QUIZ_DATA_DIR.glob("*_quiz.json")):
                sid = f.stem.replace("_quiz", "")
                try:
                    with open(f, "r") as qf:
                        qd = json.load(qf)
                        print(f"  • {GREEN}{BOLD}{sid.upper()}{RESET}: {qd.get('title', 'Knowledge Quiz')}")
                except Exception:
                    print(f"  • {sid.upper()}")
        print()
        sys.exit(0)

    run_quiz(args.session, force_reset=args.reset)

if __name__ == "__main__":
    main()
