%
% castordoc.cls
%
% LaTeX style for castor documentation
%
% written by sebastien.ponce@cern.ch, inspired from
% lhcbnote.cls (See http://lhcb-comp.web.cern.ch/lhcb-comp/Support/LateX/lhcbnote.tex)
%
\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{castordoc}[]
\LoadClass[twoside,a4paper]{report}
\RequirePackage{calc}
\RequirePackage{array}
\RequirePackage{a4wide}
\RequirePackage{fancyhdr}
\RequirePackage{graphicx}
\renewcommand\chapter{\if@openright\cleardoublepage\else\clearpage\fi
                    \thispagestyle{fancy}%
                    \global\@topnum\z@
                    \@afterindentfalse
                    \secdef\@chapter\@schapter}

\def\@docref{\mbox{-}}
\def\@docrev{\mbox{-}}
\def\@dociss{\mbox{-}}
\def\@docmod{\mbox{-}}
\def\@doccre{\mbox{-}}
\def\@doctyp{\mbox{}}
\newcommand{\docref}[1]{\def\@docref{\mbox{#1}}}
\newcommand{\docrev}[1]{\def\@docrev{\mbox{#1}}}
\newcommand{\dociss}[1]{\def\@dociss{\mbox{#1}}}
\newcommand{\docmod}[1]{\def\@docmod{\mbox{#1}}}
\newcommand{\doccre}[1]{\def\@doccre{\mbox{#1}}}
\newcommand{\doctyp}[1]{\def\@doctyp{\mbox{#1}}}

\setlength{\voffset}{-1in}
\setlength{\headheight}{2cm}
\renewcommand{\footrulewidth}{0.4pt}
\addtolength{\textheight}{1cm}
%%
\def\ps@myheadings{%
    \let\@oddfoot\@empty\let\@evenfoot\@empty
    \def\@evenhead{\thepage\hfil\slshape\leftmark}%
    \def\@oddhead{{\slshape\rightmark}\hfil\thepage}%
    \let\@mkboth\@gobbletwo
    \let\chaptermark\@gobble
    \let\sectionmark\@gobble
    }
  \if@titlepage
    \renewcommand\maketitle{\begin{titlepage}%
      \let\footnotesize\small
      \let\footnoterule\relax
      \let \footnote \thanks
      \includegraphics[width=2cm]{images/CERNLogo} \hfill
      \includegraphics[width=4cm]{images/CASTOR_logo_250}
      \null\vfil
      \vskip 60\p@
      \begin{center}%
        {\Huge \@title \par}%
        \vskip 3em%
          {\large
           \lineskip .75em%
           \begin{tabular}[t]{c}%
             \@author
           \end{tabular}\par}%
        \vskip 1.5em%
        {\large \@date \par}%       % Set date in \large size.
      \end{center}\par
      \@thanks
      \vfil\null
	{\Large \@doctyp}

	\begin{tabular}[b]{ll}
          Issue :         & \@dociss \\
          Revision :      & \@docrev \\
          Reference :     & \@docref \\
          Created :       & \@doccre \\
          Last modified : & \@docmod \\
	\end{tabular}
      \end{titlepage}%
      \setcounter{footnote}{0}%
      \global\let\thanks\relax
      \global\let\maketitle\relax
      \global\let\@thanks\@empty
      \global\let\@author\@empty
      \global\let\@date\@empty
      \global\let\@title\@empty
      \global\let\title\relax
      \global\let\author\relax
      \global\let\date\relax
      \global\let\and\relax}
  \else
    \renewcommand\maketitle{\par
      \begingroup
        \renewcommand\thefootnote{\@fnsymbol\c@footnote}%
        \def\@makefnmark{\rlap{\@textsuperscript{\normalfont\@thefnmark}}}%
        \long\def\@makefntext##1{\parindent 1em\noindent
              \hb@xtt@1.8em{%
                  \hss\@textsuperscript{\normalfont\@thefnmark}}##1}%
        \if@twocolumn
          \ifnum \col@number=\@ne
            \@maketitle
          \else
            \twocolumn[\@maketitle]%
          \fi
        \else
          \newpage
          \global\@topnum\z@   % Prevents figures from going at top of page.
          \@maketitle
        \fi
        \thispagestyle{plain}\@thanks
      \endgroup
      \setcounter{footnote}{0}%
      \global\let\thanks\relax
      \global\let\maketitle\relax
      \global\let\@maketitle\relax
      \global\let\@thanks\@empty
      \global\let\@author\@empty
      \global\let\@date\@empty
      \global\let\@title\@empty
      \global\let\title\relax
      \global\let\author\relax
      \global\let\date\relax
      \global\let\and\relax}
    \def\@maketitle{%
      \newpage
      \null
      \vskip 2em%
      \begin{center}%
        \let \footnote \thanks
        {\HUGE \@title \par}%
        \vskip 1.5em%
        {\large
         \lineskip .5em%
         \begin{tabular}[t]{c}%
           \@author
         \end{tabular}\par}%
        \vskip 1em%
        {\large \@date}%
      \end{center}%
      \par
      \vskip 1.5em}
  \fi


\pagestyle{fancy}
\addtolength{\headwidth}{\marginparsep}
\addtolength{\headwidth}{\marginparwidth}
\newcommand{\clearemptydoublepage}{\newpage{\pagestyle{empty}\cleardoublepage}}
\renewcommand{\chaptermark}[1]{\markboth{#1}{}}
\renewcommand{\sectionmark}[1]{\markright{\thesection\ #1}}
\rhead[\fancyplain{}{\bfseries\scriptsize\mbox{%
              \begin{tabular}{@{}l@{}}
              Title: \@title\\%
              \@doctyp\\%
              Issue: \@dociss\\%
              \leftmark\end{tabular}}}]
        {\fancyplain{}{\bfseries\scriptsize\mbox{%
              \begin{tabular}{@{}l@{}}
              \setlength{\tabcolsep}{0pt}%
              Title: \@title\\%
              \@doctyp\\%
              Issue: \@dociss\\%
              \leftmark\end{tabular}}}}
\lhead[\fancyplain{}{\bfseries\scriptsize\mbox{%
              \begin{tabular}{@{}lr@{}}%
                Reference:     & \@docref\\% 
                Revision:      & \@docrev\\%
                Last modified: & \@docmod\\
                               &\end{tabular}}}]
        {\fancyplain{}{\bfseries\scriptsize\mbox{%
              \begin{tabular}{@{}lr@{}}%
                Reference:     & \@docref\\% 
                Revision:      & \@docrev\\%
                Last modified: & \@docmod\\
                               &\end{tabular}}}}
\lfoot[\fancyplain{}{\bfseries\scriptsize page \thepage}]{}
\rfoot[]{\fancyplain{}{\bfseries\scriptsize page \thepage}}
\cfoot{}
\newenvironment{CompactList}
{\begin{list}{}{
  \setlength{\leftmargin}{0.5cm}
  \setlength{\itemsep}{0pt}
  \setlength{\parsep}{0pt}
  \setlength{\topsep}{0pt}
  \renewcommand{\makelabel}{}}}
{\end{list}}
\newenvironment{CompactItemize}
{
  \begin{itemize}
  \setlength{\itemsep}{-3pt}
  \setlength{\parsep}{0pt}
  \setlength{\topsep}{0pt}
  \setlength{\partopsep}{0pt}
}
{\end{itemize}}
\newcommand{\PBS}[1]{\let\temp=\\#1\let\\=\temp}
\newlength{\tmplength}
\newenvironment{TabularC}[1]
{
\setlength{\tmplength}
     {\linewidth/(#1)-\tabcolsep*2-\arrayrulewidth*(#1+1)/(#1)}
      \par\begin{tabular*}{\linewidth}
             {*{#1}{|>{\PBS\raggedright\hspace{0pt}}p{\the\tmplength}}|}
}
{\end{tabular*}\par}
\newcommand{\entrylabel}[1]{
   {\parbox[b]{\labelwidth-4pt}{\makebox[0pt][l]{\textbf{#1}}\\}}}
\newenvironment{Desc}
{\begin{list}{}
  {
    \settowidth{\labelwidth}{40pt}
    \setlength{\leftmargin}{\labelwidth}
    \setlength{\parsep}{0pt}
    \setlength{\itemsep}{-4pt}
    \renewcommand{\makelabel}{\entrylabel}
  }
}
{\end{list}}
\newenvironment{Indent}
  {\begin{list}{}{\setlength{\leftmargin}{0.5cm}}
      \item[]\ignorespaces}
  {\unskip\end{list}}
\setlength{\parindent}{0cm}
\setlength{\parskip}{0.2cm}
\addtocounter{secnumdepth}{2}
\sloppy
